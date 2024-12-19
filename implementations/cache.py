import logging
import time
import uuid
from typing import Optional, List, Any

import redis


class RedisSerializer:
    def __init__(self, redis_client : redis.Redis):
        self._redis_client = redis_client
        self._suffix = "-redis-serializer-reference"

    def serialize_data(self, parent_key, data):
        if data is None:
            return "<nil>"
        if isinstance(data, (str, int, float)):
            return data
        elif isinstance(data, list):
            list_key = f"{parent_key}:{uuid.uuid4()}{self.get_suffix()}"
            if data:
                for item in data:
                    item_key = self.serialize_data(list_key, item)
                    self._redis_client.lpush(list_key, item_key)
            else:
                list_key = "[]"
            return list_key
        elif isinstance(data, dict):
            dict_key = f"{parent_key}:{uuid.uuid4()}{self.get_suffix()}"
            if data.keys():
                for k, v in data.items():
                    item_key = self.serialize_data(dict_key, v)
                    self._redis_client.hset(dict_key, k, item_key)
            else:
                dict_key = "{}"
            return dict_key
        else:
            raise ValueError("Unsupported data type")

    def get_suffix(self):
        return self._suffix

    def deserialize_data(self, data_key, clear=False):
        if isinstance(data_key, str) and data_key.endswith(self.get_suffix()):
            data_type = self._redis_client.type(data_key)
            if data_type == "string":
                value = self._redis_client.get(data_key)
                if clear:
                    self._redis_client.delete(data_key)
                return value
            elif data_type == "list":
                value = [self.deserialize_data(item, clear=clear) for item in self._redis_client.lrange(data_key, 0, -1)]
                if clear:
                    self._redis_client.delete(data_key)
                return value
            elif data_type == "hash":
                value = {k: self.deserialize_data(v, clear=clear) for k, v in self._redis_client.hgetall(data_key).items()}
                if clear:
                    self._redis_client.delete(data_key)
                return value
            else:
                raise ValueError("Unsupported data type")
        else:
            value = data_key
            if data_key == "[]":
                value = []
            elif data_key == "{}":
                value = {}
            elif data_key == "<nil>":
                value = None
            return value

class RedisCache:
    def __init__(
            self,
            redis_client : redis.Redis,
            url_queue_key,
            processed_urls_list_key,
            page_list_key,
            deletion_candidates_key,
            failed_tries_key,
            robots_key_prefix,
            domain_next_crawl_key_prefix,
            domain_crawl_delay_key_prefix,
            robots_cache_ttl,
            max_in_memory_time,
    ):
        self._redis_client = redis_client
        self._serializer = RedisSerializer(redis_client)
        self._processed_urls_list_key = processed_urls_list_key
        self._url_queue_key = url_queue_key
        self._page_list_key = page_list_key
        self._deletion_candidates_key = deletion_candidates_key
        self._failed_tries_list_key = failed_tries_key
        self._robots_key_prefix = robots_key_prefix
        self._domain_next_crawl_key_prefix = domain_next_crawl_key_prefix
        self._domain_crawl_delay_key_prefix = domain_crawl_delay_key_prefix
        self._robots_cache_ttl = robots_cache_ttl
        self._max_in_memory_time = max_in_memory_time

    def _add_to_a_list_serialize(self, queue_key, data) -> Any:
        if isinstance(data, (str, int, float, bool)):  # Primitive types
            self._redis_client.lpush(queue_key, data)
            return data
        else:  # Complex types (list, dict, etc.)
            serialized_data = self._serializer.serialize_data(queue_key, data)
            self._redis_client.lpush(queue_key, serialized_data)
            return serialized_data

    # Queue management methods
    def put_url(self, *urls):
        self._redis_client.lpush(self._url_queue_key, *urls)

    def pop_url(self) -> Optional[List[str]]:
        return self._redis_client.rpop(self._url_queue_key)

    def add_processed_url(self, *urls):
        return self._redis_client.lpush(self._processed_urls_list_key,*urls)

    def pop_all_processed_urls(self, token) -> List[str]:
        urls = self._redis_client.lpop(
            self._processed_urls_list_key,
            self._redis_client.llen(self._processed_urls_list_key)
        )
        if not urls:
            urls = []
        return urls
    def pop_all_urls(self) -> List[str]:
        urls = self._redis_client.lpop(self._url_queue_key, self._redis_client.llen(self._url_queue_key))
        if not urls:
            urls = []
        return urls

    def get_urls_count(self) -> int:
        return self._redis_client.llen(self._url_queue_key)

    # Crawled pages data management
    def add_page(self, *data: dict):
        for d in data:
            # Horrendous but no other way to properly store all data in redis
            # Json serializing could be a solution but will also "loop" as our own serialization and will not be optimized for redis
            self._add_to_a_list_serialize(self._page_list_key, d)

    def pop_all_pages(self) -> List[dict]:
        page_list = self._redis_client.rpop(self._page_list_key, self._redis_client.llen(self._page_list_key))
        if not page_list:
            page_list = []
        return [self._serializer.deserialize_data(p, clear=True) for p in page_list]

    def get_pages_count(self) -> int:
        return self._redis_client.llen(self._page_list_key)

    # Deletion candidates methods
    def add_deletion_candidate(self, *page_urls: str):
        self._redis_client.sadd(self._deletion_candidates_key, *page_urls)

    def remove_deletion_candidate(self, *page_urls: str):
        self._redis_client.srem(self._deletion_candidates_key, *page_urls)

    def pop_all_deletion_candidates(self) -> List[str]:
        deletion_candidates = self._redis_client.spop(
            self._deletion_candidates_key,
            self._redis_client.scard(self._deletion_candidates_key)
        )
        if not deletion_candidates:
            deletion_candidates = []
        return deletion_candidates

    def get_deletion_candidates_count(self) -> int:
        return self._redis_client.scard(self._deletion_candidates_key)

    # Failed pages methods
    def add_failed_try(self, *page_urls: str):
        self._redis_client.lpush(self._failed_tries_list_key, *page_urls)

    def remove_failed_try(self, *page_urls: str):
        for p in page_urls:
            self._redis_client.lrem(self._failed_tries_list_key, 0, p)

    def pop_all_failed_tries(self) -> List[str]:
        failed_tries =  self._redis_client.rpop(self._failed_tries_list_key, self._redis_client.llen(self._failed_tries_list_key))
        if not failed_tries:
            failed_crawls = []
        return failed_tries

    def get_failed_tries_count(self) -> int:
        return self._redis_client.llen(self._failed_tries_list_key)

    # Domain-specific data management
    def set_robots_txt_content(self, domain: str, robots_txt: str, ex=None):
        robots_key = f"{self._robots_key_prefix}:{domain}"
        self._redis_client.set(robots_key, robots_txt, ex=ex or self._robots_cache_ttl)

    def get_robots_txt_content(self, domain: str) -> Optional[str]:
        robots_key = f"{self._robots_key_prefix}:{domain}"
        return self._redis_client.get(robots_key)

    def set_next_crawl_time(self, domain: str, timestamp: float, ex=None):
        if ex is not None and ex > self._max_in_memory_time:
            ex = None
        next_crawl_key = f"{self._domain_next_crawl_key_prefix}:{domain}"
        self._redis_client.set(next_crawl_key, timestamp, ex=ex)

    def get_next_crawl_time(self, domain: str) -> Optional[int]:
        next_crawl_key = f"{self._domain_next_crawl_key_prefix}:{domain}"
        return self._redis_client.get(next_crawl_key)
