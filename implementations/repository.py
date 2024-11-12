import logging
import traceback

import psycopg
import redis

from interfaces import AbstractCache, AbstractBackend, AbstractCrawlDataRepository, AbstractLock

class CrawlDataRepository(AbstractCrawlDataRepository):
    """Répertoire des données de crawl qui encapsule toutes les interactions avec les données"""

    def __init__(self, config : dict, cache: AbstractCache, backend: AbstractBackend, lock : AbstractLock):
        """
        Initializes the crawl data repository with a cache and a backend manager.
        Sets a maximum batch size for page processing and a minimum queue size.
        """
        self.cache = cache
        self.backend = backend
        self.lock = lock

        self.batch_size = config["crawler_batch_size"]
        self.min_queue_size = config["crawler_min_queue_size"]

    def _batch(self) -> bool:
        """
        Processes pages, deletion candidates, and failed crawls in batches.
        Ensures that only one process can execute the batch at a time by using a lock.
        """
        logging.debug("Attempting to acquire lock for batch operation.")

        # Check if the lock is already held
        if self.lock.locked():
            logging.info("Lock is already held. Exiting batch operation.")
            return False

        try:
            # Attempt to acquire the lock
            if not self.lock.acquire(blocking=False):
                logging.info("Failed to acquire lock. Exiting batch operation.")
                return False

            logging.debug("Lock acquired. Starting batch operation.")
            pages = self.cache.pop_all_pages()
            deletion_candidates = self.cache.pop_all_deletion_candidates()
            failed_pages = self.cache.pop_all_failed_crawls()

            if not pages and not deletion_candidates and not failed_pages:
                logging.info("Nothing to batch.")
                return False

            failed = {}
            for url in failed_pages:
                failed[url] = failed.get(url, 0) + 1

            self.backend.begin_transaction()
            if pages:
                logging.debug("Batch inserting pages.")
                self.backend.insert_pages(pages)
            if deletion_candidates:
                logging.debug("Batch deleting pages.")
                self.backend.delete_pages(deletion_candidates)
            if failed_pages:
                logging.debug("Batch updating failed crawl counters.")
                self.backend.increment_failed_crawl_counter(failed)
            self.backend.end_transaction(commit=True)

            logging.info(f"Successfully inserted {len(pages)} pages.")
            logging.info(f"Successfully deleted {len(deletion_candidates)} URLs.")
            logging.info(f"Successfully incremented {len(failed_pages)} failed URLs.")
        finally:
            # Ensure the lock is released regardless of what happens
            self.lock.release()
            logging.debug("Lock released after batch operation.")

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
        """
        Retrieves a URL from the cache. If the queue size is below the minimum threshold,
        fetches more URLs from the backend to replenish the queue.
        Ensures that only one process can access the backend at a time using a lock.
        """
        cache_count = self.cache.get_urls_count()
        need_fetch = cache_count <= self.min_queue_size

        if need_fetch:
            logging.debug("Attempting to acquire lock for pop_url fetch operation.")

            locked = self.lock.locked()

            if cache_count <= 0 and locked:
                logging.debug("Lock is already held and queue is empty. Exiting pop_url fetch operation.")
                return None

            if not locked:
                try:
                    if not self.lock.acquire(blocking=False):
                        logging.debug("Failed to acquire lock. Exiting pop_url fetch operation.")
                        return None

                    logging.debug("Lock acquired for pop_url operation. Retrieving URLs to replenish the queue.")

                    try:
                        self.backend.begin_transaction()
                        urls = self.backend.get_urls(self.batch_size)

                        if urls:
                            logging.debug("Marking URLs as queued.")
                            self.backend.set_urls_as_queued(urls)
                            logging.debug("Adding URLs to the cache.")
                            self.cache.put_url(*urls)

                        self.backend.end_transaction(commit=True)

                    except redis.RedisError as e:
                        self.backend.end_transaction(commit=False)
                        logging.error(f"Redis error while replenishing queue propagating error")
                        raise e

                    except psycopg.Error:
                        if self.backend.in_transaction():
                            self.backend.end_transaction(commit=False)
                finally:
                    # Ensure the lock is released regardless of what happens
                    self.lock.release()
                    logging.debug("Lock released after pop_url fetch operation.")

        # So here we have only (no fetch needed) and (fetch needed but lock is acquired and queue not empty)
        logging.debug("Returning a popped URL from the cache.")
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
