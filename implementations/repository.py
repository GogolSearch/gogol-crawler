import logging
import traceback

import psycopg
import redis

from interfaces import AbstractCache, AbstractBackend
from interfaces.repository import AbstractCrawlDataRepository


class CrawlDataRepository(AbstractCrawlDataRepository):
    """Répertoire des données de crawl qui encapsule toutes les interactions avec les données"""

    def __init__(self, config : dict, cache: AbstractCache, backend: AbstractBackend):
        """
        Initialise le répertoire de données de crawl avec un gestionnaire de base de données (db_manager)
        et définit une taille maximale de récupération (max_pull_size) pour les lots de pages.
        """
        self.cache = cache
        self.backend = backend

        self.batch_size = config["crawler_batch_size"]
        self.min_queue_size = config["crawler_min_queue_size"]

    def _batch(self) -> bool:
        logging.debug("Starting batch.")
        pages = self.cache.pop_all_pages()
        deletion_candidates = self.cache.pop_all_deletion_candidates()
        failed_pages = self.cache.pop_all_failed_crawls()

        if not pages and not deletion_candidates and not failed_pages:
            logging.info("Nothing to batch")
            return False

        failed = {}

        for i in failed_pages:
            if i not in failed:
                failed[i] = 1
            else:
                failed[i] += 1
        try:
            self.backend.begin_transaction()
            if pages:
                logging.debug("Batch page insert")
                self.backend.insert_pages(pages)
            if deletion_candidates:
                logging.debug("Batch delete")
                self.backend.delete_pages(deletion_candidates)
            if failed_pages:
                logging.debug("Batch failed crawl")
                self.backend.increment_failed_crawl_counter(failed)
            self.backend.end_transaction(commit=True)
        except psycopg.Error as e:
            logging.error(f"Could not batch:\n{traceback.format_exc()}")
            self.backend.end_transaction(commit=False)
            if pages:
                logging.debug("Adding page back to cache")
                self.cache.add_page(*pages)
            if deletion_candidates:
                logging.debug("Adding deletion candidates back to cache")
                self.cache.add_deletion_candidate(*deletion_candidates)
            if failed_pages:
                logging.debug("Adding failed crawl back to cache")
                self.cache.add_failed_crawl(*failed_pages)
            raise e
        logging.info(f"Successfully inserted {len(pages)} pages.")
        logging.info(f"Successfully deleted {len(deletion_candidates)} urls.")
        logging.info(f"Successfully incremented {len(failed_pages)} pages.")
        return True

    def insert_page_data(self, url, title, description, content, metadata, links):
        """Buffers insert operation in Redis and executes bulk insert when buffer is full."""
        data = {
            "url": url,
            "title": title,
            "description": description,
            "content": content,
            "metadata": metadata,
            "links": links
        }

        self.cache.add_page(data)
        if self.cache.get_pages_count() >= self.batch_size:
            self._batch()

    def delete_page(self, *urls):
        """Buffers delete operation in Redis and executes bulk delete when buffer is full."""

        self.cache.add_deletion_candidate(*urls)

    def add_failed_crawl(self, *urls):
        """Buffers failed crawl increment in Redis and executes in bulk when buffer is full."""
        self.cache.add_failed_crawl(*urls)

    def pop_url(self):
        if self.cache.get_urls_count() <= self.min_queue_size:
            logging.debug("Retrieving URLs for queue")
            try:
                self.backend.begin_transaction()
                logging.debug("Get URLs")
                urls = self.backend.get_urls(self.batch_size)
                if urls:
                    logging.debug("Set URLs as queued")
                    self.backend.set_urls_as_queued(urls)
                    logging.debug("Put URLs in cache")
                    self.cache.put_url(*urls)
                logging.debug("Ending transaction")
                self.backend.end_transaction(commit=True)
            except redis.RedisError:
                logging.error(f"Redis error, rolling back:\n{traceback.format_exc()}")
                self.backend.end_transaction(commit=False)
        logging.debug("Returning Popped URL")
        return self.cache.pop_url()

    def put_url(self, *urls : str):
        self.cache.put_url(*urls)

    def seed_if_needed(self, *urls : str) -> bool:
        """Returns true if seed was needed false otherwise"""
        if not self.backend.get_urls(self.batch_size):
            self.cache.put_url(*urls)
            return True
        return False

    def force_batch(self):
        return self._batch()

    def close(self):
        self._batch()
        urls = None
        try:
            urls = self.cache.pop_all_urls()
            self.backend.release_urls(urls)
        except psycopg.Error as e:
            self.cache.put_url(*tuple(urls))
            logging.error(f"Could not clear urls:\n{traceback.format_exc()}")
