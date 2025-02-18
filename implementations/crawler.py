import logging
import re
import traceback
import time

import html
from urllib.parse import urlparse, urljoin

import playwright
import requests
from nltk import sent_tokenize
from playwright.sync_api import sync_playwright

from constants import DJANGO_URL_VALIDATION
from implementations.repository import CrawlDataRepository
from implementations.exceptions import HTTPError
from implementations.ratelimiter import RateLimiter
from implementations.robots import RobotsTxtManager


class Crawler:
    """
    Represents a crawler that manages crawling processes, accesses robots.txt files, handles caching,
    and rate limiting, all using an automated browser (Playwright).

    Attributes:
        config (dict): The config dictionary
        seed_list (list): Initial list of URLs or seeds for the crawler to start processing.
        repository (AbstractCrawlDataRepository): Repository instance for storing and accessing crawl data.
        rate_limiter (AbstractRateLimiter): Manages the rate-limiting process to control request frequency.
        robots_manager (AbstractRobotsTxtManager): Manages access to and compliance with robots.txt files.
        max_description_length (int): Maximum length of text descriptions to be extracted during crawling.
        browser (Browser): Instance of the automated browser (Playwright).
        context (BrowserContext): Browser context for each session to manage browser-specific settings.
        running (bool): Flag indicating whether the crawler is actively running.
    """

    def __init__(
            self,
            repository: CrawlDataRepository,
            rate_limiter: RateLimiter,
            robots_manager: RobotsTxtManager,
            seed_list,

            config):
        """
        Initializes a new Crawler instance with the specified repository, cache, and seed list.

        Parameters:
            repository (CrawlDataRepository): Repository for storing and retrieving crawl data.
            rate_limiter (RateLimiter): Manages the rate-limiting process to control request frequency.
            robots_manager (RobotsTxtManager): Manages access to and compliance with robots.txt files.
            rate_limiter (RateLimiter): Manages the rate-limiting process to control request frequency.
            seed_list (list): List of initial URLs or seeds for the crawler to process.
            config (dict): The configuration dictionary.
        """
        # Initialize crawler components
        self.config = config
        self.seed_list = seed_list
        self.repository = repository
        self.robots_manager = robots_manager
        self.rate_limiter = rate_limiter


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
        content = None
        icon = None
        text = None
        links = None
        metadata = None
        redirect_type = None
        rules = None

        domain = urlparse(url).netloc

        try:
            # Check rate limiting for the domain
            if not self.rate_limiter.can_request(domain):
                logging.debug(f"Rate-limited, retry later: {url}")
                self.repository.put_url(url)  # Add the URL to the queue for retry
                return

            logging.info(f"Starting process for page {url}")

            # Check robots.txt rules for the domain

            allowed = self.robots_manager.is_url_allowed(domain, url)
            if not allowed:
                logging.debug(f"Blocked by robots.txt: {url}")
                self.repository.delete_url(url)
                return
        except requests.exceptions.RequestException:
            logging.error(f"Request Exception for URL {url}:\n{traceback.format_exc()}")
            self.repository.add_failed_try(url)
            return

        # Fetch and process the page content
        logging.debug("Starting fetch content")

        try:

            new_url, title, content, icon, links, metadata, redirect_type, canonical = self.fetch_and_parse_content(url)

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
            try:
                canonicals = self.filter_links([canonical], domain)
            except requests.exceptions.RequestException:
                canonicals = []
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

        try:
            # Filter links based on robots.txt rules and domain
            links = self.filter_links(links, domain)
        except requests.exceptions.RequestException:
            links = links

        # Handle "nosnippet" and generate the best snippet
        snippet = None if nosnippet else self.generate_snippet(content, max_snippet_value)
        # If no snippet generated, fallback to a part of the content
        if not snippet:
            snippet = content[:max_snippet_value]  # Take a part of the content

        # Insert data into the repository
        description = metadata.get("description", snippet)

        title, description, content, metadata = self.sanitize_data(title, description, content, metadata)

        if content is None:
            logging.error(f"Content was None for {url} adding a failed try.")
            self.repository.add_failed_try(url)
            self.repository.add_failed_try(old_url)
            return

        data = {
            "url": new_url,
            "old_url": old_url,
            "canonical_url": canonical_url,
            "redirect_type": redirect_type,
            "title": title,
            "description": description,
            "content": content,
            "icon": icon,
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
                - text (str): The cleaned text from the page.
                - icon (str): the page icon
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
            # Create a response handler to catch redirects and their status codes
            def handle_response(r):
                nonlocal redirect_type
                if r.status in [301, 302, 303, 307, 308]:  # Check for redirects
                    redirect_type = r.status

            page.on("response", handle_response)
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
            icon = self.get_favicon_url(page)

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

            return new_url, title, text, icon, links, metadata, redirect_type, canonical

        finally:
            page.close()

    def get_favicon_url(self, page):
        icon = None
        favicon = page.query_selector('link[rel="icon"]')
        base_url = page.url

        if favicon:
            icon_url = favicon.get_attribute('href')
            icon = urljoin(base_url, icon_url) if icon_url else None
        else:
            try:
                icon_url = urljoin(base_url, "/favicon.ico")

                response = requests.get(icon_url, timeout=5, headers={'User-Agent': self.config["browser_user_agent"]})
                if response.status_code == 200:
                    icon = icon_url
                else:
                    icon = None
            except requests.RequestException:
                icon = None
        return icon

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

    @staticmethod
    def generate_snippet(text, max_length):
        """
        Generates a snippet from the provided text.

        Args:
            text (str): The body text of the page to generate the snippet from.
            max_length (int): The maximum allowed length for the snippet.

        Returns:
            str: Snippet text.
        """
        # Tokenize the text into sentences
        sentences = sent_tokenize(text)

        # Initialize the snippet content
        snippet = ""

        for sentence in sentences:
            # Check if adding the next sentence would exceed max_length
            if len(snippet) + len(sentence) <= max_length:
                snippet += sentence + " "
            else:
                break

        # Trim the snippet to the max length if needed
        snippet = snippet.strip()

        # If the snippet is too long, truncate to max_length
        if len(snippet) > max_length:
            snippet = snippet[:max_length].rsplit(' ', 1)[0] + "..."

        return snippet

    def stop(self):
        """
        Stops the current crawling process.
        """
        self.running = False
        logging.info("Stopped main loop.")


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
        current_url = None
        while self.running:
            try:
                logging.debug("Popping a URL")
                current_url = self.repository.pop_url()

                if not current_url:
                    logging.info("No URL in cache queue. Retrieving from backend.")
                    if self.repository.force_batch():
                        continue
                    logging.info(
                        "No URL in cache queue and did not get data from the backend. Either no url is available for "
                        "now or the lock was acquired by an other crawler. Waiting...")
                    time.sleep(10)
                    continue
                self.process_page(current_url)

            except KeyboardInterrupt:
                logging.info("Manual stop...")
                if current_url:
                    self.repository.put_url(current_url)
                break

            except Exception:
                logging.error(f"An Unexpected error occurred: {traceback.format_exc()}")
                if current_url:
                    self.repository.add_failed_try(current_url)
                self.stop()

        # Waiting for other crawlers
        time.sleep(12)
        self.repository.close()
        self.close_browser()
        logging.info("Process stopped.")
