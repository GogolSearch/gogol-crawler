from abc import abstractmethod, ABC
from typing import Optional, List, Tuple


class AbstractCache(ABC):
    # Queue management methods
    @abstractmethod
    def put_url(self, *page_urls: str):
        """Add one or multiple new pages URLs to the crawl queue."""
        pass

    @abstractmethod
    def pop_url(self) -> Optional[str]:
        """Remove and return the next page URL from the crawl queue."""
        pass

    @abstractmethod
    def pop_all_urls(self) -> List[str]:
        """Return and clear all URLs currently in the queue."""
        pass

    @abstractmethod
    def get_urls_count(self) -> int:
        """Get the number of URLs in the queue."""
        pass

    @abstractmethod
    def add_page(self, *data: dict):
        """Store data for one page or multiple pages that has been successfully crawled."""
        pass

    @abstractmethod
    def pop_all_pages(self) -> List[dict]:
        """Return and clear all pages currently in the queue."""
        pass

    @abstractmethod
    def get_pages_count(self) -> int:
        """get numbers of page in list"""
        pass

    # Deletion candidates management
    @abstractmethod
    def add_deletion_candidate(self, *page_urls: str):
        """Add one or multiple URLs to the list of pages to delete."""
        pass

    @abstractmethod
    def pop_all_deletion_candidates(self) -> List[str]:
        """Returns and clear all URLs in the deletion candidates list."""
        pass

    @abstractmethod
    def remove_deletion_candidate(self, *page_urls: str):
        """Remove a URL from the deletion candidates list."""
        pass

    # Failed pages management
    @abstractmethod
    def add_failed_crawl(self, *page_urls: str):
        """Add one or multiple URL to the failed pages list."""
        pass

    @abstractmethod
    def remove_failed_crawl(self, *page_urls: str):
        """Remove a URL from the failed pages list."""
        pass

    @abstractmethod
    def pop_all_failed_crawls(self) -> List[str]:
        """Return and clear all URLs in the failed pages list."""
        pass

    # Domain-specific data management
    @abstractmethod
    def set_robots_txt_content(self, domain: str, robots_txt: str):
        """Store robots.txt rules for a specific domain."""
        pass

    @abstractmethod
    def get_robots_txt_content(self, domain: str) -> Optional[str]:
        """Retrieve stored robots.txt rules for a specific domain."""
        pass

    @abstractmethod
    def set_next_crawl_time(self, domain: str, timestamp: float, ex=None):
        """Store the next allowed crawl time for a specific domain."""
        pass

    @abstractmethod
    def get_next_crawl_time(self, domain: str) -> Optional[float]:
        """Get the next allowed crawl time for a specific domain."""
        pass

    @abstractmethod
    def set_crawl_delay(self, domain: str, delay: int):
        """Store the crawl delay for a specific domain."""
        pass

    @abstractmethod
    def get_crawl_delay(self, domain: str) -> Optional[int]:
        """Retrieve the crawl delay for a specific domain."""
        pass