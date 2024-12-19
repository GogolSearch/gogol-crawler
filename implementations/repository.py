import logging
import time
import traceback
import uuid
from typing import Dict

import psycopg
import redis

from implementations import PostgreSQLBackend, RedisLock, RedisCache


class CrawlDataRepository:
    """Répertoire des données de crawl qui encapsule toutes les interactions avec les données"""

    def __init__(
            self,
            cache: RedisCache,
            backend: PostgreSQLBackend,
            lock : RedisLock,
            token: str,
            batch_size : int,
            queue_min_size : int,
            failed_tries_max_size : int,
            deletion_candidates_max_size : int
    ):
        """
        Initializes the crawl data repository with a cache and a backend manager.
        Sets a maximum batch size for page processing and a minimum queue size.
        """
        self._cache = cache
        self._backend = backend
        self._lock = lock
        self._token = token

        self._batch_size = batch_size
        self._min_queue_size = queue_min_size
        self._failed_tries_max_size = failed_tries_max_size
        self._deletion_candidates_max_size = deletion_candidates_max_size


    def _release_urls(self):
        if self._lock.owned():
            self._lock.release()
            logging.debug("Released lock in batch operation, lock was already owned")

        logging.debug("Attempting to acquire lock for url release operation.")

        # Check if the lock is already held
        if self._lock.locked():
            logging.debug(f"Lock is already held. Exiting url release operation. token : {self._token}")
            return False

        urls = []
        try:
            # Attempt to acquire the lock
            if self._lock.acquire(blocking=False, token=self._token):
                urls = self._cache.pop_all_urls()
                if urls:
                    self._backend.release_urls(urls)
            else:
                logging.debug(f"Lock is already held while trying in _release_urls operation. token : {self._token}")
        except psycopg.errors.Error:
            self._cache.put_url(*tuple(urls))
            logging.error(f"Could not clear urls:\n{traceback.format_exc()}")
        finally:
            if self._lock.owned():
                self._lock.release()
                logging.debug("Lock released after _release_urls operation")

    def _batch_release_processed_urls(self, urls):
        logging.debug("Batch releasing processed urls")
        page_start = time.time()
        self._backend.release_urls(urls)
        page_end = time.time()
        logging.info(f"Successfully released {len(urls)} processed urls in {page_end - page_start} seconds.")

    def _batch_pages(self, pages):
        logging.debug("Batch inserting pages.")
        page_start = time.time()
        self._backend.insert_pages(pages)
        page_end = time.time()
        logging.info(f"Successfully inserted {len(pages)} pages in {page_end - page_start} seconds.")

    def _batch_failed_tries(self, failed_pages):
        logging.debug("Batch updating failed tries.")
        failed = {}
        for url in failed_pages:
            failed[url] = failed.get(url, 0) + 1

        failed_pages_start = time.time()
        self._backend.increment_failed_tries(failed)
        failed_pages_end = time.time()
        logging.info(f"Successfully incremented {len(failed_pages)} failed URLs in {failed_pages_end - failed_pages_start} seconds.")

    def _batch_deletion_candidates(self, deletion_candidates):
        logging.debug("Batch deleting deletion candidates.")
        deletion_candidates_start = time.time()
        self._backend.delete_urls(deletion_candidates)
        deletion_candidates_end = time.time()
        logging.info(f"Successfully deleted {len(deletion_candidates)} URLs in {deletion_candidates_end - deletion_candidates_start} seconds..")

    def _batch(self) -> bool:
        """
        Processes pages, deletion candidates, and failed crawls in batches.
        Ensures that only one process can execute the batch at a time by using a lock.
        """
        if self._lock.owned():
            self._lock.release()
            logging.debug("Released lock in batch operation, lock was already owned")

        succeed = False
        pages = []
        failed_pages = []
        deletion_candidates = []
        processed_urls = []

        logging.debug("Attempting to acquire lock for batch operation.")
        try:
            # Attempt to acquire the lock
            if self._lock.acquire(blocking=False, token=self._token):

                logging.debug("Lock acquired. Starting batch operation.")

                pages = self._cache.pop_all_pages()
                failed_tries = self._cache.pop_all_failed_tries()
                deletion_candidates = self._cache.pop_all_deletion_candidates()
                processed_urls = self._cache.pop_all_processed_urls(self._token)

                self._backend.begin_transaction()
                start = time.time()
                if pages:
                    self._batch_pages(pages)
                else:
                    logging.debug("No pages to batch insert")

                if failed_tries:
                    self._batch_failed_tries(failed_tries)
                else:
                    logging.debug("No failed tries to batch update")

                if processed_urls:
                    self._batch_release_processed_urls(processed_urls)
                else:
                    logging.debug("No processed urls to batch release")

                if deletion_candidates:
                    self._batch_deletion_candidates(deletion_candidates)
                else:
                    logging.debug("No deletion candidates to batch delete")

                self._backend.end_transaction(commit=True)
                end = time.time()

                if pages or failed_tries or deletion_candidates or processed_urls:
                    logging.info(f"Successfully realised all needed operations in {end - start} seconds.")
                else:
                    logging.info("No batch operations to perform")
                succeed = True
            else:
                logging.debug(f"Lock is already held while trying in batch operation. token : {self._token}")
        except psycopg.errors.Error:
            logging.error(f"Could not batch:\n{traceback.format_exc()}")
            self._backend.end_transaction(commit=False)
            if pages:
                self._cache.add_page(*pages)
            if deletion_candidates:
                self._cache.add_deletion_candidate(*deletion_candidates)
            if failed_pages:
                    self._cache.add_failed_try(*failed_pages)
            if processed_urls:
                self._cache.add_processed_url(*processed_urls)
        finally:
            # Ensure the lock is released regardless of what happens
            if self._lock.owned():
                self._lock.release()
                logging.debug("Lock released after batch operation.")
        return succeed

    def insert_page_data(self, page_data : Dict):
        """Buffers insert operation in Redis and executes bulk insert when buffer is full."""

        self._cache.add_page(page_data)
        self._cache.remove_failed_try(page_data["url"])
        self._cache.remove_deletion_candidate(page_data["url"])
        if self._cache.get_pages_count() >= self._batch_size:
            self._batch()

    def delete_url(self, *urls):
        """Buffers delete operation in Redis and executes bulk delete when buffer is full."""

        self._cache.add_deletion_candidate(*urls)
        if self._cache.get_deletion_candidates_count() >= self._deletion_candidates_max_size:
            self._batch()

    def add_failed_try(self, *urls):
        """Buffers failed crawl increment in Redis and executes in bulk when buffer is full."""
        self._cache.add_failed_try(*urls)
        if self._cache.get_failed_tries_count() >= self._failed_tries_max_size:
            self._batch()

    def pop_url(self):
        """
        Retrieves a URL from the cache. If the queue size is below the minimum threshold,
        fetches more URLs from the backend to replenish the queue.
        Ensures that only one process can access the backend at a time using a lock.
        """
        cache_count = self._cache.get_urls_count()
        need_fetch = cache_count <= self._min_queue_size

        if need_fetch:
            logging.debug("Attempting to acquire lock for pop_url fetch operation.")

            try:
                if self._lock.owned():
                    self._lock.release()
                    logging.debug("Released lock in pop_url fetch operation, lock was already owned")

                if self._lock.acquire(blocking=False, token=self._token):
                    logging.debug("Lock acquired for pop_url operation. Retrieving URLs to replenish the queue.")

                    self._backend.begin_transaction()
                    urls = self._backend.get_urls(self._batch_size)

                    if urls:
                        logging.debug("Marking URLs as queued.")
                        self._backend.set_urls_as_queued(urls)
                        logging.debug("Adding URLs to the cache.")
                        self._cache.put_url(*urls)

                    self._backend.end_transaction(commit=True)
                else:
                    logging.debug(f"Lock is already held while trying in pop_url fetch operation. token : {self._token}")

            except redis.RedisError as e:
                if self._backend.in_transaction():
                    self._backend.end_transaction(commit=False)
                logging.error(f"Redis error while replenishing queue propagating error")
                raise e

            except psycopg.errors.Error:
                if self._backend.in_transaction():
                    self._backend.end_transaction(commit=False)
            finally:
                # Ensure the lock is released regardless of what happens
                if self._lock.owned():
                    self._lock.release()
                    logging.debug("Lock released after pop_url fetch operation.")

        logging.debug("Returning a popped URL from the cache.")
        url = self._cache.pop_url()
        self._cache.add_processed_url(url)
        return url

    def put_url(self, *urls : str):
        self._cache.put_url(*urls)

    def seed_if_needed(self, *urls : str) -> bool:
        """Returns true if seed was needed false otherwise"""
        logging.debug("Attempting to acquire lock for seed_if_needed operation.")
        success = False
        try:
            if self._lock.owned():
                self._lock.release()
                logging.debug("Released lock in seed_if_needed operation, lock was already owned")
            if self._lock.acquire(blocking=False, token=self._token):
                logging.debug("Lock acquired for seed_if_needed operation. Checking backend for pages.")

                if not self._backend.get_urls(self._batch_size):
                    self._cache.put_url(*urls)
                    success = True
            else:
                logging.debug(f"Lock is already held while trying in seed_if_needed operation. token : {self._token}")
        finally:
            if self._lock.owned():
                self._lock.release()
        return success

    def force_batch(self):
        return self._batch()

    def close(self):
        if self._lock.owned():
            self._lock.release()
        self._batch()
        self._release_urls()
