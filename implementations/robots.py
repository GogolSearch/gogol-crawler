from protego import Protego
import requests
import logging
from typing import Optional


class RobotsTxtManager:
    """
    Manages rules from a domain's robots.txt file using the Protego library.
    """

    def __init__(self, cache, user_agent: str):
        """
        Initializes the RobotsTxtManager.

        Args:
            cache: Cache repository to store robots.txt data.
            user_agent (str): The bot's user agent.
        """
        self.cache = cache
        self.user_agent = user_agent

    def get_robots(self, domain: str) -> Optional[Protego]:
        """
        Retrieves and parses the robots.txt for the given domain.

        Args:
            domain (str): The domain to fetch robots.txt for.

        Returns:
            Protego: Parsed Protego object if successful, None otherwise.
        """
        # Check cache
        cached_robots = self.cache.get_robots_txt_content(domain)
        if cached_robots:
            if cached_robots == "<nil>":
                return None
            return Protego.parse(cached_robots)

        # Fetch robots.txt
        robots_url = f"http://{domain}/robots.txt"
        response = requests.get(robots_url, timeout=5, headers={'User-Agent': self.user_agent})
        if response.status_code == 200:
            robots_txt = response.text
            rp = Protego.parse(robots_txt)

            # Cache with expiration (crawl delay or None)
            crawl_delay = rp.crawl_delay(self.user_agent)
            ex = int(crawl_delay) if crawl_delay else None
            self.cache.set_robots_txt_content(domain, robots_txt, ex=ex)
            return rp
        else:
            self.cache.set_robots_txt_content(domain, "<nil>", ex=None)
            logging.warning(f"Failed to fetch robots.txt for {domain}: HTTP {response.status_code}")
        return None

    def is_url_allowed(self, domain: str, url: str) -> bool:
        """
        Determines if a URL is allowed based on robots.txt rules.

        Args:
            domain (str): The domain to check.
            url (str): The URL to check.

        Returns:
            bool: True if the URL is allowed, False otherwise.
        """
        robots = self.get_robots(domain)
        if not robots:
            return True  # Default to allowing access if robots.txt can't be fetched
        return robots.can_fetch(url, self.user_agent)

    def get_crawl_delay(self, domain: str) -> Optional[float]:
        """
        Retrieves the crawl delay for the domain.

        Args:
            domain (str): The domain to check.

        Returns:
            Optional[float]: The crawl delay in seconds, or None if not specified.
        """
        robots = self.get_robots(domain)
        if robots:
            return robots.crawl_delay(self.user_agent)
        return None