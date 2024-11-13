from abc import ABC, abstractmethod
from typing import Dict, List


class AbstractCrawlDataRepository(ABC):
    """Répertoire des données de crawl qui encapsule toutes les interactions avec les données"""

    @abstractmethod
    def insert_page_data(self, url : str, title :str, description : str, content : str, metadata : Dict, links : List):
        """Buffers insert operation in Redis and executes bulk insert when buffer is full."""
        pass

    @abstractmethod
    def delete_page(self, *urls : str):
        pass

    @abstractmethod
    def add_failed_try(self, *urls : str):
        pass

    @abstractmethod
    def pop_url(self) -> str:
        pass

    @abstractmethod
    def put_url(self, *urls : str):
        pass

    @abstractmethod
    def seed_if_needed(self, *urls : str):
        pass

    @abstractmethod
    def force_batch(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass