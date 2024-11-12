import logging
import traceback
from urllib.parse import urlparse

import requests

from interfaces import AbstractCache


class RobotsTxtManager:
    """
    Manages rules from a domain's robots.txt file, including caching the content
    and processing disallow and allow rules.

    Attributes:
        cache (AbstractCache): Cache repository for storing and retrieving robots.txt data.
    """

    def __init__(self, cache: AbstractCache, robots_cache_ttl=300):
        """
        Initializes the robots.txt manager.

        Args:
            cache (AbstractCache): Cache repository to store robots.txt data.
            robots_cache_ttl (int, optional): Cache TTL for the robots.txt file, defaults to 300 seconds.
        """
        self.cache = cache

    def get_rules(self, domain):
        """
        Retrieves and parses the robots.txt rules for a domain.
        If the content is not cached, fetches it via HTTP and caches it.

        Args:
            domain (str): Domain name for which to retrieve the rules.

        Returns:
            list: List of rules as tuples ("disallow"/"allow", path).
        """
        robots_txt_content = self.cache.get_robots_txt_content(domain)
        if not robots_txt_content:
            robots_txt_content = self.fetch_robots_txt(domain)
            if robots_txt_content:
                self.cache.set_robots_txt_content(domain, robots_txt_content)
                self.cache_crawl_delay(robots_txt_content, domain)
        return self.parse_rules(robots_txt_content)

    @staticmethod
    def fetch_robots_txt(domain):
        """
        Fetches the robots.txt content for a domain.

        Args:
            domain (str): Domain name for which to retrieve the robots.txt file.

        Returns:
            str: Content of the robots.txt file, or an empty string in case of an error.
        """
        response = requests.get(f"http://{domain}/robots.txt", timeout=5)
        if response.status_code == 200:
            return response.text
        else:
            logging.warning(f"No accessible robots.txt for {domain}")

    def cache_crawl_delay(self, robots_txt_content, domain):
        """
        Extracts and caches the "crawl-delay" value from the robots.txt file, if present.

        Args:
            robots_txt_content (str): Content of the robots.txt file.
            domain (str): Domain name for which to set the crawl delay.
        """
        for line in robots_txt_content.splitlines():
            if line.lower().startswith("crawl-delay:"):
                try:
                    crawl_delay = int(line.split(":", 1)[1].strip())
                    self.cache.set_crawl_delay(domain, crawl_delay)
                except ValueError:
                    logging.warning(f"Invalid crawl-delay value in robots.txt for {domain}")

    @staticmethod
    def parse_rules(robots_txt_content):
        """
        Parses allow and disallow rules from robots.txt content.

        Args:
            robots_txt_content (str): Content of the robots.txt file.

        Returns:
            list: List of rules as tuples ("disallow"/"allow", path).
        """
        rules = []
        user_agent = "*"

        for line in robots_txt_content.splitlines():
            line = line.strip()
            if line.startswith("User-agent:"):
                user_agent = line.split(":", 1)[1].strip().lower()
            elif line.startswith("Disallow:") and user_agent in ("*", "googlebot"):
                path = line.split(":", 1)[1].strip()
                rules.append(("disallow", path))
            elif line.startswith("Allow:") and user_agent in ("*", "googlebot"):
                path = line.split(":", 1)[1].strip()
                rules.append(("allow", path))

        return rules

    @staticmethod
    def is_url_allowed(rules, url):
        """
        Determines if a URL is allowed based on the provided rules.

        Args:
            rules (list): List of rules as tuples ("disallow"/"allow", path).
            url (str): URL to check.

        Returns:
            bool: True if the URL is allowed, False otherwise.
        """
        url_path = urlparse(url).path
        is_allowed = True  # By default, access is allowed

        for rule_type, path in rules:
            if url_path.startswith(path):
                is_allowed = rule_type == "allow"

        return is_allowed