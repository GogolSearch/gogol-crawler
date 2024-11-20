from datetime import datetime, timedelta
from typing import Dict, Any, List
from interfaces import AbstractBackend
import psycopg
from psycopg_pool import ConnectionPool
from psycopg.types.json import Jsonb

from datetime import datetime, timedelta
from typing import Dict, Any, List
from interfaces import AbstractBackend
import psycopg
from psycopg_pool import ConnectionPool
from psycopg.types.json import Jsonb

class PostgreSQLBackend(AbstractBackend):
    """
    PostgreSQLBackend is a backend class for interacting with a PostgreSQL database
    using a connection pool. It provides methods for handling transactions, fetching,
    updating, inserting, and deleting data related to pages in the database.

    Attributes:
        _queued_counter (int): Counter to track the number of queued operations.
        _pool (ConnectionPool): Connection pool used to manage database connections.
        _urls_table (str): Name of the table storing page data.
        _transaction_conn (Optional[psycopg.Connection]): The current active connection during a transaction.
        _transaction_cur (Optional[psycopg.Cursor]): The current active cursor during a transaction.
        _in_transaction (bool): Flag indicating whether a transaction is in progress.
    """

    def __init__(self, pool: ConnectionPool):
        """
        Initializes the PostgreSQLBackend with necessary configurations.

        Args:
            pool (ConnectionPool): A psycopg2 connection pool to manage database connections.
        """
        self._queued_counter = 0
        self._pool = pool
        self._urls_table = "urls"
        self._transaction_conn = None  # Holds the connection during a transaction
        self._transaction_cur = None  # Holds the cursor during a transaction
        self._in_transaction = False  # Flag to track transaction state

    def begin_transaction(self) -> None:
        """
        Begins a transaction by setting up a dedicated connection and cursor
        that will be used for all subsequent database operations.

        Raises:
            RuntimeError: If a transaction is already in progress.
        """
        if self._in_transaction:
            raise RuntimeError("A transaction is already in progress.")

        self._transaction_conn = self._pool.getconn()
        self._transaction_cur = self._transaction_conn.cursor()  # Create cursor here
        self._in_transaction = True

    def end_transaction(self, commit: bool = True) -> None:
        """
        Ends the transaction by either committing or rolling back all changes.

        Args:
            commit (bool): Flag to indicate whether to commit the transaction. Defaults to True.

        Raises:
            RuntimeError: If no active transaction is in progress.
        """
        if not self._in_transaction:
            raise RuntimeError("No active transaction to end.")

        try:
            if commit:
                self._transaction_conn.commit()
            else:
                self._transaction_conn.rollback()
        finally:
            # Clean up the transaction state
            self._transaction_cur.close()  # Close the cursor first
            self._pool.putconn(self._transaction_conn)  # Release the connection
            self._transaction_conn = None
            self._transaction_cur = None  # Reset the cursor
            self._in_transaction = False

    def in_transaction(self) -> bool:
        """
        Checks if a transaction is currently in progress.

        Returns:
            bool: True if a transaction is active, False otherwise.
        """
        return self._in_transaction

    def _get_connection(self) -> psycopg.Connection:
        """
        Returns the current connection to be used for operations.
        If a transaction is active, it returns the transaction connection.

        Returns:
            psycopg.Connection: The current database connection.
        """
        if self._in_transaction:
            return self._transaction_conn
        else:
            return self._pool.getconn()

    def _get_cursor(self, connection) -> psycopg.Cursor | psycopg.ServerCursor:
        """
        Returns the current cursor to be used for operations.
        If a transaction is active, it returns the transaction cursor.

        Args:
            connection (psycopg.Connection): The database connection to use for the cursor.

        Returns:
            psycopg.Cursor: The current database cursor.
        """
        if self._in_transaction:
            return self._transaction_cur
        else:
            return connection.cursor()

    def get_urls(self, batch_size: int, recrawl=False) -> List[str]:
        """
        Retrieves a list of URLs from the database that have not been crawled or
        have been crawled more than a day ago.

        Args:
            batch_size (int): The number of URLs to retrieve.

        Returns:
            List[str]: A list of URLs fetched from the database.
        """
        query = f"SELECT url FROM {self._urls_table} WHERE last_crawled_at IS NULL"
        if self._queued_counter % 3 == 0:
            query += f" OR last_crawled_at < NOW() - INTERVAL '1 DAY'"

        query += " ORDER BY id LIMIT %s"
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute(query, (batch_size,))
        rows = cursor.fetchall()

        # If not in a transaction, commit and release the connection
        if not self._in_transaction:
            connection.commit()
            cursor.close()  # Close the cursor first
            self._pool.putconn(connection)  # Release the connection back to pool

        return [r[0] for r in rows]

    def set_urls_as_queued(self, urls: List[str]) -> None:
        """
        Marks the specified URLs as queued in the database.

        Args:
            urls (List[str]): A list of URLs to mark as queued.
        """
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute(f"UPDATE {self._urls_table} set queued='true' WHERE url = ANY(%s)", (urls,))

        # If not in a transaction, commit and release the connection
        if not self._in_transaction:
            connection.commit()
            cursor.close()  # Close the cursor first
            self._pool.putconn(connection)  # Release the connection back to pool
        self._queued_counter += 1

    def release_urls(self, urls: List[str]) -> None:
        """
        Releases the specified URLs from the queued state in the database.

        Args:
            urls (List[str]): A list of URLs to release.
        """
        query = f"UPDATE {self._urls_table} SET queued = FALSE WHERE url = ANY(%s)"
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute(query, (urls,))

        if not self._in_transaction:
            connection.commit()
            cursor.close()
            self._pool.putconn(connection)

    def insert_pages(self, pages_data: List[Dict]) -> None:
        """
        Inserts a list of pages into the database using a batch insert stored procedure.

        Args:
            pages_data (List[Dict]): A list of dictionaries containing page data.
        """
        query = "CALL batch_insert_pages(%s)"
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute(query, (Jsonb(pages_data),))

        # If not in a transaction, commit and release the connection
        if not self._in_transaction:
            connection.commit()
            cursor.close()  # Close the cursor first
            self._pool.putconn(connection)  # Release the connection back to pool

    def delete_urls(self, page_urls: List[str]) -> None:
        """
        Deletes the specified urls from the database.

        Args:
            page_urls (List[str]): A list of URLs of the pages to delete.
        """
        query = f"DELETE FROM {self._urls_table} WHERE url = ANY(%s)"
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute(query, (page_urls,))

        if not self._in_transaction:
            connection.commit()
            cursor.close()
            self._pool.putconn(connection)

    def increment_failed_tries(self, fail_mapping: Dict) -> None:
        """
        Increments the failed_tries counter for the specified URLs.

        Args:
            fail_mapping (Dict): A dictionary mapping URLs to the number of failed attempts.
        """
        args = []
        for k, v in fail_mapping.items():
            args.append((v, k))
        query = f"UPDATE {self._urls_table} SET failed_tries = failed_tries + %s WHERE url = %s"
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.executemany(query, args)

        if not self._in_transaction:
            connection.commit()
            cursor.close()
            self._pool.putconn(connection)
