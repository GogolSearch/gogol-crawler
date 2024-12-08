import time
from typing import Callable

from implementations.robots import RobotsTxtManager
from interfaces import AbstractCache, AbstractLock, AbstractRobotsTxtManager
from interfaces.ratelimiter import AbstractRateLimiter


class RateLimiter(AbstractRateLimiter):
    """
    Manages rate limiting for domains, determining the wait time before the next allowed request.

    Attributes:
        cache (AbstractCache): Interface to access the cache, where rate limit information is stored.
        robots (RobotsTxtManager): Manager for robots.txt rules, including crawl delays.
        default_delay (int): Default delay in seconds between requests for a domain if no specific delay is defined.
        lock_factory (Callable[[str], AbstractLock]): Factory function to create locks for domains.
    """

    def __init__(
        self,
        cache: AbstractCache,
        robots: AbstractRobotsTxtManager,
        default_delay: int,
        lock_factory: Callable[[str], AbstractLock]
    ):
        """
        Initializes the rate-limiting manager.

        Args:
            cache (AbstractCache): Cache repository for storing rate-limiting data.
            robots (AbstractRobotsTxtManager): Manager for robots.txt rules, including crawl delays.
            default_delay (int): Default delay in seconds between requests if no other delay is specified.
            lock_factory (Callable[[str], AbstractLock]): Factory function for creating locks.
        """
        if not callable(lock_factory):
            raise ValueError("lock_factory must be a callable that returns an AbstractLock.")
        self.cache = cache
        self.robots = robots
        self.default_delay = default_delay
        self.lock_factory = lock_factory

    def can_request(self, domain: str) -> bool:
        """
        Checks if a request can be made for the given domain. If the request is allowed,
        updates the cache to set the next permitted wait time.

        Args:
            domain (str): Domain for which to check request allowance.

        Returns:
            bool: True if the request is allowed, False otherwise.
        """
        lock = self.lock_factory(domain)  # Create or retrieve a lock for the domain

        try:
            # Attempt to acquire the lock, blocking until it is available
            if not lock.acquire(blocking=False):
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
        next_allowed_request_time = self.cache.get_next_crawl_time(domain)
        current_time = time.time()

        # Deny the request if a wait time is set and hasn't been reached yet
        if next_allowed_request_time and float(next_allowed_request_time) > current_time:
            return False

        # Allow the request and set the next wait time based on crawl-delay or default delay
        crawl_delay = self.robots.get_crawl_delay(domain)
        delay = crawl_delay if crawl_delay else self.default_delay
        self.cache.set_next_crawl_time(domain, float(current_time) + float(delay), ex=int(delay))
        return True
