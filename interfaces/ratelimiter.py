from abc import abstractmethod, ABC

class AbstractRateLimiter(ABC):
    """
    Manages rate limiting for domains, determining the wait time before the next allowed request.
    """
    @abstractmethod
    def can_request(self, domain: str) -> bool:
      pass