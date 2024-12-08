from abc import ABC, abstractmethod
from typing import Optional

class AbstractRobotsTxtManager(ABC):
    """
    Manages rules from a domain's robots.txt file using the Protego library.
    """

    @abstractmethod
    def is_url_allowed(self, domain: str, url: str) -> bool:
        """
        Determines if a URL is allowed based on robots.txt rules.

        Args:
            domain (str): The domain to check.
            url (str): The URL to check.

        Returns:
            bool: True if the URL is allowed, False otherwise.
        """
        pass

    @abstractmethod
    def get_crawl_delay(self, domain: str) -> Optional[float]:
        """
        Retrieves the crawl delay for the domain.

        Args:
            domain (str): The domain to check.

        Returns:
            Optional[float]: The crawl delay in seconds, or None if not specified.
        """
        pass