import time

from implementations.robots import RobotsTxtManager
from interfaces import AbstractCache


class RateLimiter:
    """
    Manages rate limiting for domains, determining the wait time before the next allowed request.

    Attributes:
        cache (AbstractCache): Interface to access the cache, where rate limit information is stored.
        default_delay (int): Default delay in seconds between requests for a domain if no specific delay is defined.
    """

    def __init__(self, cache: AbstractCache, robots : RobotsTxtManager ,default_delay):
        """
        Initializes the rate-limiting manager.

        Args:
            cache (AbstractCache): Cache repository for storing rate-limiting data.
            default_delay (int, optional): Default delay in seconds between requests if no other delay is specified. Defaults to 3 seconds.
        """
        self.cache = cache
        self.robots = robots
        self.default_delay = default_delay

    def can_request(self, domain):
        """
        Checks if a request can be made for the given domain. If the request is allowed,
        updates the cache to set the next permitted wait time.

        Args:
            domain (str): Domain for which to check request allowance.

        Returns:
            bool: True if the request is allowed, False otherwise.
        """
        # Retrieve the next allowed request time for this domain
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
