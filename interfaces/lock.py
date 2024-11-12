from abc import ABC, abstractmethod
from typing import Optional, Union

class AbstractLock(ABC):
    """
    Abstract base class for a shared, distributed lock. The lock methods mimic those of `redis.lock.Lock` but some that are not used aren't specified even if the implementation will use them internally.
    """

    @abstractmethod
    def __enter__(self) -> AbstractLock:
        pass
        
    @abstractmethod
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        pass
    
    @abstractmethod
    def acquire(
        self,
        sleep: Optional[Union[int, float]] = None,
        blocking: Optional[bool] = None,
        blocking_timeout: Optional[Union[int, float]] = None,
        token: Optional[str] = None
    ) -> bool:
        """
        Acquire the lock.
        """
        pass
    
    @abstractmethod
    def locked(self) -> bool:
        """
        Check if the lock is held by any process.
        """
        pass
        
    @abstractmethod
    def release(self) -> None:
        """
        Release the lock.
        """
        pass
