from abc import abstractmethod, ABC
from typing import Dict, Any, Tuple

from abc import ABC, abstractmethod
from typing import List, Dict, Optional


class AbstractBackend(ABC):
    """
    Abstract class for a backend system that interacts with the crawler.
    Concrete implementations should provide specific functionality for interacting
    with the implementations system (e.g., database, Redis, etc.).
    """

    @abstractmethod
    def begin_transaction(self):
        """
        Begins a transaction by setting up a dedicated connection or handle
        that will be used for all subsequent operations.
        Subclasses should implement this method to start a transaction
        with their respective backend.

        Raises:
            RuntimeError: If a transaction is already in progress.
        """
    @abstractmethod
    def end_transaction(self, commit: bool = True):
        """
        Ends the transaction by either committing or rolling back all changes.
        Subclasses should implement this method to finalize the transaction
        with their respective backend.

        Args:
            commit (bool): Whether to commit the transaction (True) or rollback (False).

        Raises:
            RuntimeError: If no transaction is currently in progress.
        """

    @abstractmethod
    def in_transaction(self) -> bool:
        """
        returns whether the backend is in transaction or not.
        """
        pass

    @abstractmethod
    def get_urls(self, batch_size) -> List[Any]:
        """
        Retrieves a batch of pages (URLs) from the backend implementations that need to be crawled.
        They need to also be marked as queue in the backend

        Args:
            batch_size (int): The number of pages to fetch.

        Returns:
            List[Any]: A list of URLs (for now this may be changed to a dict or anything) to crawl.
        """
        pass

    @abstractmethod
    def set_urls_as_queued(self, urls : List[str]) -> None:
        """
        Mark url(s) as queued in the backend

        Args:
            urls (List[str]): The page(s) to mark as queued.
        """
        pass

    @abstractmethod
    def release_urls(self, page_urls : List[str]) -> None:
        """
        Release a batch of pages (URLs) from the backend implementations that are not in the queue anymore.

        Args:
            page_urls (List[str]): The URLS of pages to release from the queue.

        """

    @abstractmethod
    def insert_pages(self, page_data: List[Dict]) -> None:
        """
        Inserts  new pages into the backend implementations.

        Args:
            page_data (List[Dict]): The associated data to store for the pages.
        """
        pass

    @abstractmethod
    def delete_pages(self, page_urls: List[str]) -> None:
        """
        Deletes a page from the backend implementations.

        Args:
            page_urls (List[str]): The URLs of the pages to delete.
        """
        pass

    @abstractmethod
    def increment_failed_tries(self, fail_mapping: Dict) -> None:
        """
        Increments the failed crawl counter for a page in the backend implementations.

        Args:
            fail_mapping (Dict): A mapping of failed pages and their fail count
        """
        pass
