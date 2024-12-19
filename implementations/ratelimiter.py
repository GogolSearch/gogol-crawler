import logging
import time
from typing import Callable

from implementations import RedisCache
from implementations.lock import RedisLock
from implementations.robots import RobotsTxtManager


class RateLimiter:
    """
    Manages rate limiting for domains, determining the wait time before the next allowed request.

    Attributes:
        _cache (RedisCache): Interface to access the cache, where rate limit information is stored.
        _robots (RobotsTxtManager): Manager for robots.txt rules, including crawl delays.
        _token (str): the token for locks.
        _default_delay (int): Default delay in seconds between requests for a domain if no specific delay is defined.
        _lock_factory (Callable[[str], RedisLock]): Factory function to create locks for domains.
    """

    def __init__(
        self,
        cache: RedisCache,
        robots: RobotsTxtManager,
        token : str,
        default_delay: int,
        lock_factory: Callable[[str], RedisLock]
    ):
        """
        Initializes the rate-limiting manager.

        Args:
            cache (Cache): Cache repository for storing rate-limiting data.
            robots (RobotsTxtManager): Manager for robots.txt rules, including crawl delays.
            token (str): the token for locks.
            default_delay (int): Default delay in seconds between requests if no other delay is specified.
            lock_factory (Callable[[str], RedisLock]): Factory function for creating locks.
        """
        if not callable(lock_factory):
            raise ValueError("lock_factory must be a callable that returns a Lock.")
        self._cache = cache
        self._robots = robots
        self._token = token
        self._default_delay = default_delay
        self._lock_factory = lock_factory

    def can_request(self, domain: str) -> bool:
        """
        Checks if a request can be made for the given domain. If the request is allowed,
        updates the cache to set the next permitted wait time.

        Args:
            domain (str): Domain for which to check request allowance.

        Returns:
            bool: True if the request is allowed, False otherwise.
        """
        lock = self._lock_factory(domain)  # Create or retrieve a lock for the domain

        # Mitigation if somehow lock wasn't properly released by this crawler instance
        if lock.owned():
            lock.release()
            logging.debug("Released lock in ratelimiter operation, lock was already owned")

        try:
            # Attempt to acquire the lock, blocking until it is available
            if not lock.acquire(blocking=False, token=self._token):
                return False

            # Critical section: Check and update the rate-limiting cache
            return self._process_can_request(domain)

        finally:
            # Always release the lock
            if lock.owned():
                lock.release()

    def _process_can_request(self, domain: str) -> bool:
        """
        Internal helper to process the request and update the cache.

        Args:
            domain (str): The domain to process.

        Returns:
            bool: True if the request is allowed, False otherwise.
        """
        next_allowed_request_time = self._cache.get_next_crawl_time(domain)
        current_time = time.time()

        # Deny the request if a wait time is set and hasn't been reached yet
        if next_allowed_request_time and float(next_allowed_request_time) > current_time:
            return False

        # Allow the request and set the next wait time based on crawl-delay or default delay
        crawl_delay = self._robots.get_crawl_delay(domain)
        delay = crawl_delay if crawl_delay else self._default_delay
        self._cache.set_next_crawl_time(domain, float(current_time) + float(delay), ex=int(delay))
        return True
