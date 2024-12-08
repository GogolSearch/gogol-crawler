import os
import signal
import sys

import logging
import argparse
from typing import Any

import psycopg_pool
import redis

from implementations.crawler import Crawler
from implementations.lock import RedisLock
from implementations.ratelimiter import RateLimiter
from implementations.repository import CrawlDataRepository
from implementations import RedisCache, PostgreSQLBackend
from implementations.robots import RobotsTxtManager


def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in {'true', 't', 'yes', 'y', '1'}:
        return True
    elif value.lower() in {'false', 'f', 'no', 'n', '0'}:
        return False
    else:
        raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")

def get_env_variable(name, default=None) -> Any:
    """Helper to fetch environment variables with optional defaults."""
    return os.environ.get(name, default)

def configure_logging(log_level) -> None:
    """Configure logging for the application."""
    levels = logging.getLevelNamesMapping()
    logging.basicConfig(level=levels.get(log_level, logging.INFO), stream=sys.stdout)

def create_connection_pool(config) -> psycopg_pool.ConnectionPool:
    """Create and return a connection pool."""
    return psycopg_pool.ConnectionPool(
        min_size=1,
        max_size=3,
        conninfo=f"dbname={config['db_name']} user={config['db_user']} password={config['db_password']} "
                 f"host={config['db_host']} port={config['db_port']}"
    )

def create_redis_client(config) -> redis.Redis:
    """Create and return a Redis client."""
    return redis.Redis(
        host=config["redis_host"],
        port=config["redis_port"],
        db=config["redis_db"],
        encoding="utf-8",
        decode_responses=True
    )

def configure_signal_handlers(crawler, redis_client, pool) -> None:
    """Set up handlers for termination signals."""
    def handle_close(s, frame):
        logging.info("Stopping...")
        crawler.stop()
        logging.info(f"Stopped crawler {crawler}.")
        redis_client.close()
        logging.info("Stopped redis client.")
        pool.close()
        logging.info("Stopped pool.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_close)
    signal.signal(signal.SIGINT, handle_close)

def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description='Crawler configuration')

    # Database arguments
    parser.add_argument(
        '--db_host',
        type=str,
        default=get_env_variable('DB_HOST')
    )

    parser.add_argument(
        '--db_name',
        type=str,
        default=get_env_variable('DB_NAME')
    )

    parser.add_argument(
        '--db_user',
        type=str,
        default=get_env_variable('DB_USER')
    )

    parser.add_argument(
        '--db_password',
        type=str,
        default=get_env_variable('DB_PASSWORD')
    )

    parser.add_argument(
        '--db_port',
        type=int,
        default=get_env_variable('DB_PORT')
    )

    # Redis arguments
    parser.add_argument(
        '--redis_host',
        type=str,
        default=get_env_variable('REDIS_HOST')
    )

    parser.add_argument(
        '--redis_port',
        type=int,
        default=get_env_variable('REDIS_PORT')
    )

    parser.add_argument(
        '--redis_db',
        type=int,
        default=get_env_variable('REDIS_DB', default=0)
    )

    # Crawler configuration
    parser.add_argument(
        "--log_level",
        type=str,
        default=get_env_variable('LOG_LEVEL', default="INFO")
    )

    parser.add_argument(
        "--crawler_batch_size",
        type=int,
        default=get_env_variable('CRAWLER_BATCH_SIZE', default=300)
    )

    parser.add_argument(
        "--crawler_queue_min_size",
        type=int,
        default=get_env_variable('CRAWLER_QUEUE_MIN_SIZE', default=100)
    )

    parser.add_argument(
        "--crawler_deletion_candidates_max_size",
        type=int,
        default=get_env_variable('CRAWLER_DELETION_CANDIDATES_MAX_SIZE', default=100)
    )

    parser.add_argument(
        "--crawler_failed_tries_max_size",
        type=int,
        default=get_env_variable('CRAWLER_FAILED_TRIES_MAX_SIZE', default=100)
    )

    parser.add_argument(
        "--crawler_default_delay",
        type=int,
        default=get_env_variable('CRAWLER_DEFAULT_DELAY', default=3)
    )

    parser.add_argument(
        "--crawler_max_description_length",
        type=int,
        default=get_env_variable('CRAWLER_MAX_DESCRIPTION_LENGTH', default=300)
    )

    # Browser configuration
    browser_headless = get_env_variable('BROWSER_HEADLESS', default="true").lower() in ['true', 't']

    parser.add_argument(
        "--browser_headless",
        type=str_to_bool,
        default=browser_headless
    )

    parser.add_argument(
        "--browser_port",
        type=int,
        default=get_env_variable('BROWSER_PORT', default=4444)
    )

    parser.add_argument(
        "--browser_host",
        type=str,
        default=get_env_variable('BROWSER_HOST', default="localhost")
    )

    parser.add_argument(
        "--browser_user_agent",
        type=str,
        default=get_env_variable('BROWSER_USER_AGENT', default="GogolBot/SchoolProject")
    )

    parser.add_argument(
        "--browser_remote",
        type=str_to_bool,
        default=get_env_variable('BROWSER_REMOTE', default="false").lower() in ['true']
    )

    parser.add_argument(
        "--lock_name",
        type=str,
        default=get_env_variable('LOCK_NAME', default="crawler:db_lock")
    )

    parser.add_argument(
        "--domain_lock_prefix",
        type=str,
        default=get_env_variable('DOMAIN_LOCK_PREFIX', default="crawler:domain_lock")
    )

    # Cache arguments
    parser.add_argument(
        "--cache_url_queue_key",
        type=str,
        default=get_env_variable('CACHE_URL_QUEUE_KEY', default="crawler:urls_queue")
    )

    parser.add_argument(
        "--cache_page_list_key",
        type=str,
        default=get_env_variable('CACHE_PAGE_LIST_KEY', default="crawler:pages_list")
    )

    parser.add_argument(
        "--cache_deletion_candidates_key",
        type=str,
        default=get_env_variable('CACHE_DELETION_CANDIDATES_KEY', default="crawler:deletion_candidates_list")
    )

    parser.add_argument(
        "--cache_failed_tries_list_key",
        type=str,
        default=get_env_variable('CACHE_FAILED_TRIES_LIST_KEY', default="crawler:failed_tries_list")
    )

    parser.add_argument(
        "--cache_robots_key_prefix",
        type=str,
        default=get_env_variable('CACHE_ROBOTS_KEY_PREFIX', default="crawler:robots")
    )

    parser.add_argument(
        "--cache_domain_next_crawl_key_prefix",
        type=str,
        default=get_env_variable('CACHE_DOMAIN_NEXT_CRAWL_KEY_PREFIX', default="crawler:next_crawl")
    )

    parser.add_argument(
        "--cache_domain_crawl_delay_key_prefix",
        type=str,
        default=get_env_variable('CACHE_DOMAIN_CRAWL_DELAY_KEY_PREFIX', default="crawler:crawl_delay")
    )

    parser.add_argument(
        "--cache_robots_cache_ttl",
        type=int,
        default=get_env_variable('CACHE_ROBOTS_CACHE_TTL', default=300)
    )

    parser.add_argument(
        "--cache_max_in_memory_time",
        type=int,
        default=get_env_variable('CACHE_MAX_IN_MEMORY_TIME', default=600)
    )

    # Parse and return the arguments
    return parser.parse_args()

def main() -> None:
    args = parse_args()

    config = vars(args)  # Convert Namespace to dict

    for k, v in config.items():
        if v is None:
            raise ValueError(f"key: {k} is required")

    # Configure logging
    configure_logging(config["log_level"])

    if config["browser_remote"]:
        os.environ["SELENIUM_REMOTE_URL"] = f"http://{config["browser_host"]}:{config["browser_port"]}/wd/hub/"

    pool = create_connection_pool(config)
    redis_client = create_redis_client(config)

    cache = RedisCache(
        redis_client,
        config["cache_url_queue_key"],
        config["cache_page_list_key"],
        config["cache_deletion_candidates_key"],
        config["cache_failed_tries_list_key"],
        config["cache_robots_key_prefix"],
        config["cache_domain_next_crawl_key_prefix"],
        config["cache_domain_crawl_delay_key_prefix"],
        config["cache_robots_cache_ttl"],
        config["cache_max_in_memory_time"],
    )

    backend = PostgreSQLBackend(pool)
    lock = RedisLock(redis_client, config["lock_name"])
    cdr = CrawlDataRepository(
        cache,
        backend,
        lock,
        config["crawler_batch_size"],
        config["crawler_queue_min_size"],
        config["crawler_failed_tries_max_size"],
        config["crawler_deletion_candidates_max_size"],
        )
    robots = RobotsTxtManager(cache, config["browser_user_agent"])
    rate_limiter = RateLimiter(cache, robots, config["crawler_default_delay"], lambda domain : RedisLock(redis_client, config["domain_lock_prefix"] + ":" + domain))

    # Define seed list for crawling
    seed_list = [
        "https://www.wikipedia.org",
        "https://www.archive.org",
        "https://www.nytimes.com",
        "https://www.bbc.com",
        "https://www.reddit.com",
        "https://www.github.com",
        "https://www.stackoverflow.com",
        "https://www.medium.com",
        "https://www.imdb.com",
    ]
    c = Crawler(cdr, rate_limiter, robots, seed_list, config)

    configure_signal_handlers(c, redis_client, pool)

    c.run()

if __name__ == "__main__":
    main()
