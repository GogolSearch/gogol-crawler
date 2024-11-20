import logging
import re
import traceback
import time
from typing import List

import html
from urllib.parse import urlparse, urljoin

import playwright
import requests
from nltk import sent_tokenize
from playwright.sync_api import sync_playwright

from constants import DJANGO_URL_VALIDATION
from implementations.exceptions import HTTPError
from implementations.ratelimiter import RateLimiter
from implementations.robots import RobotsTxtManager
from interfaces import AbstractCrawlDataRepository, AbstractCache


class Crawler:
    """
    Represents a crawler that manages crawling processes, accesses robots.txt files, handles caching,
    and rate limiting, all using an automated browser (Playwright).

    Attributes:
        config (dict): The config dictionary
        seed_list (list): Initial list of URLs or seeds for the crawler to start processing.
        repository (AbstractCrawlDataRepository): Repository instance for storing and accessing crawl data.
        rate_limiter (RateLimiter): Manages the rate-limiting process to control request frequency.
        robots_manager (RobotsTxtManager): Manages access to and compliance with robots.txt files.
        max_description_length (int): Maximum length of text descriptions to be extracted during crawling.
        browser (Browser): Instance of the automated browser (Playwright).
        context (BrowserContext): Browser context for each session to manage browser-specific settings.
        running (bool): Flag indicating whether the crawler is actively running.
    """

    def __init__(self, repository: AbstractCrawlDataRepository, cache: AbstractCache, seed_list, config):
        """
        Initializes a new Crawler instance with the specified repository, cache, and seed list.

        Parameters:
            repository (AbstractCrawlDataRepository): Repository for storing and retrieving crawl data.
            cache (AbstractCache): Cache system used for managing rate limiting and other temporary data.
            seed_list (list): List of initial URLs or seeds for the crawler to process.
            config (dict): The configuration dictionary.
        """
        # Initialize crawler components
        self.config = config
        self.seed_list = seed_list
        self.repository = repository
        self.robots_manager = RobotsTxtManager(cache, self.config["browser_user_agent"])
        self.rate_limiter = RateLimiter(cache, self.robots_manager, self.config["crawler_default_delay"])


        # Default keys and parameters
        self.max_description_length = config["crawler_max_description_length"]  # Maximum length for extracted descriptions
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
        old_url = None
        canonical_url = None
        canonicals = None
        new_url = None
        title = None
        text = None
        links = None
        metadata = None
        redirect_type = None
        rules = None

        domain = urlparse(url).netloc

        # Check rate limiting for the domain
        if not self.rate_limiter.can_request(domain):
            logging.debug(f"Rate-limited, retry later: {url}")
            self.repository.put_url(url)  # Add the URL to the queue for retry
            return
            
        logging.info(f"Starting process for page {url}")

        # Check robots.txt rules for the domain
        try:
            allowed = self.robots_manager.is_url_allowed(domain, url)
            if not allowed:
                logging.debug(f"Blocked by robots.txt: {url}")
                self.repository.delete_url(url)
                return
        except (requests.exceptions.InvalidURL, requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            logging.error(f"Connection error or invalid URL for {url}:\n{traceback.format_exc()}")
            self.repository.add_failed_try(url)
            return

        # Fetch and process the page content
        logging.debug("Starting fetch content")

        try:

            new_url, title, content, links, metadata, redirect_type, canonical = self.fetch_and_parse_content(url)

        except playwright.sync_api.Error:
            logging.error(f"Playwright error while fetching content{url}:\n{traceback.format_exc()}")
            self.repository.add_failed_try(url)
            return False

        except HTTPError as e:
            logging.error(f"URL {url} returned an error code {e.status_code}, skipping crawl.")
            self.repository.add_failed_try(url)
            return False

        logging.debug("Successfully fetched content.")

        if new_url != url:
            old_url = url

        if canonical:
            canonicals = self.filter_links([canonical], domain)
        if canonicals:
            canonical_url = canonicals[0]


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
        snippet = None if nosnippet else self.generate_best_snippet(content, title, max_snippet_value)

        # Insert data into the repository
        description = metadata.get("description", snippet)

        title, description, content, metadata = self.sanitize_data(title, description, content, metadata)

        if content is None:
            logging.error(f"Description or content was None for {url} adding a failed try.")
            self.repository.add_failed_try(url)
            return

        data = {
            "url": new_url,
            "old_url": old_url,
            "canonical_url": canonical_url,
            "redirect_type": redirect_type,
            "title": title,
            "description": description,
            "content": content,
            "metadata": metadata,
            "links": links
        }
        self.repository.insert_page_data(data)
        logging.debug("Page was put in page list")
        return True

    def filter_links(self, links, current_domain):
        """
        Filters links based on regex and robots.txt rules for links belonging to the same domain.

        Args:
            links (list): List of links to filter.
            current_domain (str): The current domain of the page to check against the rules.
            current_rules (str): The current domain robots txt rules

        Returns:
            list: List of filtered links based on robots.txt rules.
        """
        regex = re.compile(DJANGO_URL_VALIDATION, re.IGNORECASE)
        filtered_links = []
        for link in links:
            if re.match(regex, link):
                link_domain = urlparse(link).netloc
                if link_domain == current_domain:  # Check only links on the same domain
                    if self.robots_manager.is_url_allowed(current_domain, link):
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
                - canonical (str or None): The canonical link of the page, if present.

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
            links = [l for l in links if len(l) <= 2048]

            # Extract the canonical link
            canonical_element = page.query_selector('link[rel="canonical"]')
            canonical = urljoin(new_url, canonical_element.get_attribute('href')) if canonical_element else None

            return new_url, title, text, links, metadata, redirect_type, canonical

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

        logging.debug("Seeding if needed")
        self.repository.seed_if_needed(*self.seed_list)

        logging.debug("Main loop starting")
        while self.running:
            try:
                logging.debug("Popping a URL")
                url = self.repository.pop_url()

                if not url:
                    logging.info("No URL in cache queue. Retrieving from backend.")
                    if self.repository.force_batch():
                        continue
                    logging.info(
                        "No URL in cache queue and did not get data from the backend. Either no url is available for "
                        "now or the lock was acquired by an other crawler. Waiting...")
                    time.sleep(10)
                    continue

                self.process_page(url)

            except Exception:
                logging.error(f"An Unexpected error occurred: {traceback.format_exc()}")
                continue

            except KeyboardInterrupt:
                logging.info("Manual stop...")
                self.stop()

        logging.info("Process stopped.")
