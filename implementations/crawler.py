import html
import logging
import re
import traceback
import time
from datetime import datetime
from urllib.parse import urlparse, urljoin

import playwright
import requests
from nltk import sent_tokenize
from playwright.sync_api import sync_playwright

from implementations.exceptions import HTTPError
from implementations.ratelimiter import RateLimiter
from implementations.robots import RobotsTxtManager
from interfaces import AbstractCrawlDataRepository, AbstractCache


class Crawler:
    """
    Represents a crawler that manages crawling processes, accesses robots.txt files, handles caching,
    and rate limiting, all using an automated browser (Playwright).

    Attributes:
        repository (AbstractCrawlDataRepository): Repository instance for storing and accessing crawl data.
        rate_limiter (RateLimiter): Manages the rate-limiting process to control request frequency.
        robots_manager (RobotsTxtManager): Manages access to and compliance with robots.txt files.
        seed_list (list): Initial list of URLs or seeds for the crawler to start processing.
        browser (Browser): Instance of the automated browser (Playwright).
        context (BrowserContext): Browser context for each session to manage browser-specific settings.
        max_description_length (int): Maximum length of text descriptions to be extracted during crawling.
        running (bool): Flag indicating whether the crawler is actively running.
    """

    def __init__(self, repository: AbstractCrawlDataRepository, cache: AbstractCache, seed_list, config):
        """
        Initializes a new Crawler instance with the specified repository, cache, and seed list.

        Parameters:
            repository (AbstractCrawlDataRepository): Repository for storing and retrieving crawl data.
            cache (AbstractCache): Cache system used for managing rate limiting and other temporary data.
            seed_list (list): List of initial URLs or seeds for the crawler to process.
        """
        # Initialize crawler components
        self.config = config
        self.seed_list = seed_list
        self.repository = repository
        self.rate_limiter = RateLimiter(cache, self.config["crawler_default_delay"])
        self.robots_manager = RobotsTxtManager(cache)

        # Default keys and parameters
        self.max_description_length = 300  # Maximum length for extracted descriptions
        self.browser = None
        self.context = None
        self.running = True

    def start_browser(self):
        """
        Starts the Playwright browser and creates a new browser instance and context.

        Uses Playwright to launch a headless Chromium browser and set up a session context.
        """
        # Start Playwright and launch the browser
        p = sync_playwright().start()
        self.browser = p.chromium.launch(headless=self.config["browser_headless"])
        self.context = self.browser.new_context(user_agent=self.config["browser_user_agent"])
        logging.info("Browser started.")

    def close_browser(self):
        """
        Closes the associated browser and context.

        If the browser is running, it closes the session context and the browser itself.
        """
        if self.browser:
            logging.info("Trying to close browser.")
            self.context.close()  # Close the session context
            logging.info("Browser context closed")
            self.browser.close()  # Close the browser
            logging.info("Browser closed.")

    def process_page(self, url):
        """
        Processes a web page by checking restrictions (rate limits, robots.txt), retrieving
        its content, processing it, and saving it in the repository.

        Args:
            url (str): The URL of the page to process.

        Returns:
            bool: Returns True if processing succeeded, otherwise False.
        """
        logging.info(f"Starting process for page {url} at {datetime.now():%Y-%m-%d %H:%M:%S}")
        domain = urlparse(url).netloc

        # Check rate limiting for the domain
        if not self.rate_limiter.can_request(domain):
            logging.debug(f"Rate-limited, retry later: {url}")
            self.repository.put_url(url)  # Add the URL to the queue for retry
            return False

        # Check robots.txt rules for the domain
        rules = None
        try:
            rules = self.robots_manager.get_rules(domain)
        except (requests.exceptions.InvalidURL, requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            logging.error(f"Connection error or invalid URL for {url}:\n{traceback.format_exc()}")
            self.repository.add_failed_crawl(url)
            return False

        if not self.robots_manager.is_url_allowed(rules, url):
            logging.debug(f"Blocked by robots.txt: {url}")
            self.repository.delete_page(url)
            return False

        # Fetch and process the page content
        try:
            logging.debug("Starting fetch content")
            new_url, title, text, links, metadata, redirect_type = self.fetch_and_parse_content(url)
            logging.debug("Successfully fetched content.")

            # Handle redirects
            if new_url != url:
                if redirect_type == 301:  # Permanent redirect
                    logging.debug(f"Page URL permanently redirected, deleting: {url} -> {new_url}")
                    self.repository.delete_page(url)
                else:  # Temporary redirect
                    logging.debug(f"Page URL temporarily redirected from {url} to {new_url}")
                    new_url = url  # Use the original URL if temporary redirect

            # Extract robots.txt directives for page processing
            noindex = "noindex" in metadata.get("robots", "").lower()
            nofollow = "nofollow" in metadata.get("robots", "").lower()
            nosnippet = "nosnippet" in metadata.get("robots", "").lower()
            max_snippet_value = self.get_max_snippet_length(metadata.get("robots", ""))

            # Handle pages marked "noindex" or "nofollow"
            if noindex:
                logging.info(f"Page marked as noindex: {url}")

            if nofollow:
                logging.info(f"Page marked as nofollow, removing links: {url}")
                links = []  # Clear all links if nofollow is present

            # Filter links based on robots.txt rules and domain
            links = self.filter_links(links, domain)

            # Handle "nosnippet" and generate the best snippet
            snippet = None if nosnippet else self.generate_best_snippet(text, title, max_snippet_value)

            # Insert data into the repository
            description = metadata.get("description", snippet)

            title, description, text, metadata = self.sanitize_data(title, description, text, metadata)

            self.repository.insert_page_data(new_url, title, description, text, metadata, links)

            logging.debug("Page data inserted")
            return True

        except playwright.sync_api.Error:
            logging.error(f"Playwright error while processing {url}:\n{traceback.format_exc()}")
            self.repository.add_failed_crawl(url)
            return False
        except HTTPError as e:
            logging.error(f"URL {url} returned an error code {e.status_code}, skipping crawl.")
            self.repository.add_failed_crawl(url)
            return False
        except Exception:
            logging.error(f"Error processing {url}:\n{traceback.format_exc()}")
            return False

    def filter_links(self, links, current_domain):
        """
        Filters links based on robots.txt rules for links belonging to the same domain.

        Args:
            links (list): List of links to filter.
            current_domain (str): The current domain of the page to check against the rules.

        Returns:
            list: List of filtered links based on robots.txt rules.
        """
        filtered_links = []
        for link in links:
            link_domain = urlparse(link).netloc
            if link_domain == current_domain:  # Check only links on the same domain
                link_rules = self.robots_manager.get_rules(link_domain)
                if self.robots_manager.is_url_allowed(link_rules, link):
                    filtered_links.append(link)
            else:
                filtered_links.append(link)  # Add links from other domains without checking
        return filtered_links

    def fetch_and_parse_content(self, url):
        """
        Retrieves and parses the content of a page using Playwright, handles redirects,
        extracts metadata, text, links, and detects HTTP errors.

        Args:
            url (str): The URL of the page to retrieve and parse.

        Returns:
            tuple: A tuple containing the following information:
                - new_url (str): The final URL after redirection (if applicable).
                - title (str): The title of the page.
                - clean_text (str): The cleaned text from the page.
                - links (list): A list of links extracted from the page.
                - metadata (dict): A dictionary containing metadata extracted from the page.
                - redirect_type (int or None): The type of HTTP redirection (if any).

        Raises:
            Exception: If an error occurs during the download or processing of the page.
        """
        if not self.browser:
            raise Exception("The browser is not started.")

        page = self.context.new_page()
        redirect_type = None  # Initialize the redirect type

        try:
            # Function to intercept HTTP requests and detect redirects
            def handle_route(route, request):
                nonlocal redirect_type

                if route.response:
                    # If the response is a redirect (HTTP code in the 3xx range), capture the redirect type
                    if 300 <= route.response.status < 400:
                        redirect_type = route.response.status
                route.continue_()  # Continue the request (even after interception)

            # Add the route handler to capture responses
            page.on('route', handle_route)

            # Navigate to the URL
            response = page.goto(url)

            # Wait for the page to fully load
            try:
                page.wait_for_load_state('networkidle', timeout=10000)
            except playwright.sync_api.TimeoutError:
                # Log the timeout and continue
                logging.warning(f"Timeout while waiting for 'networkidle' for {url}. Proceeding without full load.")

            # If a redirection occurred, `new_url` will contain the final destination URL
            new_url = page.url  # Final URL after redirection (if any)

            # If the status code of the final page is an error (4xx or 5xx), skip the crawl
            if response.status >= 400:
                raise HTTPError(response.status, new_url)

            # Continue with normal page processing if the status code is OK
            title = page.title()

            # Extract metadata directly with Playwright
            metadata = self.extract_metadata(page)

            # Extract visible text and links from the page
            text = page.inner_text('body')

            links = [
                urljoin(new_url, a.get_attribute('href'))
                for a in page.query_selector_all('a[href]')
                if a.get_attribute('href')  # Include only links that have a 'href' attribute
            ]

            return new_url, title, text, links, metadata, redirect_type

        except Exception as e:
            logging.error(f"Error retrieving and parsing content for {url}:\n{traceback.format_exc()}")
            raise e
        finally:
            page.close()

    @staticmethod
    def sanitize_data(title, description, text, metadata) -> tuple:
        """
        Sanitize by escaping html.

        Args:
            title (str): The title of the page.
            description (str): The description of the page.
            text (str): The text of the page.
            metadata (dict): A dictionary containing metadata extracted from the page.

        Returns:
            str: The cleaned text.
        """
        # Using a regular expression to keep only alphanumeric characters and spaces
        title = html.escape(title)
        description = html.escape(description)
        text = html.escape(text)
        metadata = {k: html.escape(v) for k,v in metadata.items()}

        return title, description, text, metadata

    def get_max_snippet_length(self, robots_content):
        """
        Extracts the `max-snippet` value from the robots metadata content and returns the maximum snippet length.
        If no limit is specified, returns the default maximum length.

        Args:
            robots_content (str): The content of the `robots` field in the page metadata.

        Returns:
            int: The maximum snippet length to use. Returns `-1` if no limit is set.
        """
        # Searching for the 'max-snippet' directive in the robots metadata
        match = re.search(r'max-snippet:(-?\d+)', robots_content)
        if match:
            # Extracting and converting the value to an integer
            description_length = int(match.group(1))
            # If the specified length is less than or equal to the allowed maximum length
            if description_length <= self.max_description_length:
                return description_length

        # If no specific limit is found, return the default maximum length
        return self.max_description_length

    @staticmethod
    def score_sentences(sentences, title):
        """
        Evaluates each sentence based on its relevance to the page title and returns the sentences
        sorted by descending relevance score.

        Relevance is determined by several factors:
        - Sentences containing keywords from the title are scored higher.
        - Sentences appearing earlier in the text are considered more important.
        - Longer sentences, which provide more context, are scored higher.

        Args:
            sentences (list): A list of sentences (strings) to evaluate.
            title (str): The title of the page, used to extract keywords.

        Returns:
            list: A list of tuples, each containing a sentence and its relevance score.
                  The list is sorted by score in descending order.
        """
        scores = []

        # Convert the title to lowercase to compare with the keywords in the sentences
        title_keywords = title.lower().split()

        for sentence in sentences:
            score = 0

            # Add points if the sentence contains title keywords
            for keyword in title_keywords:
                if keyword in sentence.lower():
                    score += 2  # Increase score for each keyword found in the sentence

            # Calculate position score: the closer the sentence is to the start, the more important it is
            position_score = max(0, int(1 - (sentences.index(sentence) / len(sentences))))
            score += position_score

            # Calculate score based on sentence length: longer sentences are rated higher
            score += min(5, int(len(sentence) / 100))

            scores.append((sentence, score))

        # Sort sentences by relevance score in descending order
        return sorted(scores, key=lambda x: x[1], reverse=True)

    @staticmethod
    def extract_metadata(page):
        """
        Extracts relevant metadata from a webpage using Playwright.

        Args:
            page (playwright.page.Page): The Playwright page object representing the loaded webpage.

        Returns:
            dict: A dictionary containing the extracted metadata. Keys may include 'description',
                  'robots', and 'rating'.
        """
        metadata = {}

        # Extract all meta tags with Playwright
        meta_tags = page.query_selector_all('meta')

        for meta in meta_tags:
            name = meta.get_attribute('name')
            content = meta.get_attribute('content')

            if not name or not content:
                continue

            name = name.lower()

            # Store relevant metadata
            if name == 'description':
                metadata['description'] = content
            elif name == 'robots':
                metadata['robots'] = content
            elif name == 'rating':
                metadata['rating'] = content

        return metadata

    def generate_best_snippet(self, text, title, max_length):
        """
        Generates the best snippet by evaluating sentences and selecting the most relevant one.
        The snippet is chosen based on sentence relevance and length constraints.

        Args:
            text (str): The body text of the page to generate the snippet from.
            title (str): The title of the page, used to extract keywords for relevance evaluation.
            max_length (int): The maximum allowed length for the snippet.

        Returns:
            str: The best snippet, ensuring it fits within the specified maximum length.
        """
        # Tokenize the text into sentences
        sentences = sent_tokenize(text)

        # Clean and filter sentences to remove excessive whitespace
        sentences = [re.sub(r'\s+', ' ', sentence.strip()) for sentence in sentences]

        # Evaluate sentences based on their relevance to the title
        scored_sentences = self.score_sentences(sentences, title)

        # Select the best sentence or combination of sentences
        best_snippet = ''
        for sentence, score in scored_sentences:
            if len(best_snippet) + len(sentence) > max_length:
                break
            best_snippet += sentence + " "

        # Truncate to ensure the snippet fits within the maximum length
        if len(best_snippet) > max_length:
            best_snippet = best_snippet[:max_length].rsplit(' ', 1)[0]

        return best_snippet.strip()

    def stop(self):
        """
        Stops the current crawling process, halts the browser, and clears the pages in the queue.

        Ensures that the crawling operation is properly terminated, and resources such as the browser and
        repository are cleaned up.
        """
        self.running = False
        logging.info("Stopped main loop.")
        self.repository.close()
        self.close_browser()

    def run(self):
        """
        Starts the crawling process. It initiates the browser, populates the queue, and then
        begins processing pages in the queue. The process continues until it is stopped.

        This method runs in a loop, continuously processing URLs from the queue and handling
        exceptions or stopping conditions.
        """
        logging.info("Running...")
        logging.debug("Starting browser")
        self.start_browser()

        logging.info("Seeding if needed")
        self.repository.seed_if_needed(*self.seed_list)

        logging.debug("Main loop starting")
        while self.running:
            try:
                logging.debug("Popping a URL")
                url = self.repository.pop_url()

                if not url:
                    logging.info("No URL in queue. Waiting...")
                    if self.repository.force_batch():
                        continue
                    logging.info(
                        "No URL in queue (cache and backend), and no URL available in cache. Waiting for some time.")
                    time.sleep(1800)
                    continue  # Wait for new URLs in cache

                self.process_page(url)

            except Exception:
                logging.error(f"Error occurred: {traceback.format_exc()}")
                self.stop()

            except KeyboardInterrupt:
                logging.info("Manual stop...")
                self.stop()

        logging.info("Process stopped.")