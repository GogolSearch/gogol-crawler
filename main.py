import os
import signal
import sys

import logging
import argparse

import redis

from psycopg_pool import ConnectionPool

from implementations.crawler import Crawler
from implementations.lock import RedisLock
from implementations.repository import CrawlDataRepository
from implementations import RedisCache, PostgreSQLBackend

def get_env_variable(name, default=None):
    """Helper to fetch environment variables with optional defaults."""
    return os.environ.get(name, default)

def configure_logging(log_level):
    """Configure logging for the application."""
    levels = logging.getLevelNamesMapping()
    logging.basicConfig(level=levels.get(log_level, logging.INFO), stream=sys.stdout)

def create_connection_pool(config):
    """Create and return a connection pool."""
    return ConnectionPool(
        min_size=1,
        max_size=3,
        conninfo=f"dbname={config['db_name']} user={config['db_user']} password={config['db_password']} "
                 f"host={config['db_host']} port={config['db_port']}"
    )

def create_redis_client(config):
    """Create and return a Redis client."""
    return redis.Redis(
        host=config["redis_host"],
        port=config["redis_port"],
        db=config["redis_db"],
        encoding="utf-8",
        decode_responses=True
    )

def configure_signal_handlers(crawler, redis_client, pool):
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

def parse_args():
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description='Crawler configuration')

    # Database arguments
    parser.add_argument('--db_host', type=str, default=get_env_variable('DB_HOST'))
    parser.add_argument('--db_name', type=str, default=get_env_variable('DB_NAME'))
    parser.add_argument('--db_user', type=str, default=get_env_variable('DB_USER'))
    parser.add_argument('--db_password', type=str, default=get_env_variable('DB_PASSWORD'))
    parser.add_argument('--db_port', type=int, default=get_env_variable('DB_PORT'))

    # Redis arguments
    parser.add_argument('--redis_host', type=str, default=get_env_variable('REDIS_HOST'))
    parser.add_argument('--redis_port', type=int, default=get_env_variable('REDIS_PORT'))
    parser.add_argument('--redis_db', type=int, default=get_env_variable('REDIS_DB', default=0))

    # Crawler configuration
    parser.add_argument("--log_level", type=str, default=get_env_variable('LOG_LEVEL', default="INFO"))
    parser.add_argument("--crawler_batch_size", type=int, default=get_env_variable('CRAWLER_BATCH_SIZE', default=20000))
    parser.add_argument("--crawler_min_queue_size", type=int, default=get_env_variable('CRAWLER_MIN_QUEUE_SIZE', default=50))
    parser.add_argument("--crawler_default_delay", type=int, default=get_env_variable('CRAWLER_DEFAULT_DELAY', default=3))

    # Browser configuration
    browser_headless = get_env_variable('BROWSER_HEADLESS', default="true").lower() in ['true', 't']
    parser.add_argument("--browser_headless", type=bool, default=browser_headless)
    parser.add_argument("--browser_port", type=int, default=get_env_variable('BROWSER_PORT', default=4444))
    parser.add_argument("--browser_host", type=str, default=get_env_variable('BROWSER_HOST', default="localhost"))
    parser.add_argument("--browser_user_agent", type=str, default=get_env_variable('BROWSER_USER_AGENT', default="GogolBot/SchoolProject"))
    parser.add_argument("--browser_remote", type=bool, default=get_env_variable('BROWSER_REMOTE', default="false").lower() in ['true'])

    parser.add_argument("--lock_name", type=str, default=get_env_variable('LOCK_NAME', default="crawler:db_lock"))

    # Parse and return the arguments
    return parser.parse_args()

def main():
    args = parse_args()

    config = vars(args)  # Convert Namespace to dict

    for k, v in config.items():
        if v is None:
            raise ValueError(f"key: {k} is required")
    # Configure logging
    configure_logging(config["log_level"])

    if config["browser_remote"]:
        os.environ["SELENIUM_REMOTE_URL"] = f"http://{config["browser_host"]}:{config["browser_port"]}/wd/hub/"
    # Set up connections and resources
    pool = create_connection_pool(config)
    redis_client = create_redis_client(config)
    cache = RedisCache(redis_client)
    backend = PostgreSQLBackend(pool)
    lock = RedisLock(redis_client, config["lock_name"])
    cdr = CrawlDataRepository(config, cache, backend, lock)

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
    c = Crawler(cdr, cache, seed_list, config)

    # Configure signal handlers (this is now only in the main thread)
    configure_signal_handlers(c, redis_client, pool)

    c.run()

if __name__ == "__main__":
    main()
