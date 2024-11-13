from datetime import datetime, timedelta
from typing import Dict, Any, List
from interfaces import AbstractBackend
import psycopg
from psycopg_pool import ConnectionPool
from psycopg.types.json import Jsonb

class PostgreSQLBackend(AbstractBackend):
    def __init__(self, pool: ConnectionPool):
        """
        Initializes the PostgreSQLBackend with necessary configurations.

        Args:
            pool: A psycopg2 connection pool.
        """
        self._queued_counter = 0
        self._pool = pool
        self._pages_table = "pages"
        self._transaction_conn = None  # Holds the connection during a transaction
        self._transaction_cur = None  # Holds the cursor during a transaction
        self._in_transaction = False  # Flag to track transaction state

    def begin_transaction(self):
        """
        Begins a transaction by setting up a dedicated connection and cursor
        that will be used for all subsequent database operations.
        """
        if self._in_transaction:
            raise RuntimeError("A transaction is already in progress.")

        self._transaction_conn: psycopg.Connection = self._pool.getconn()
        self._transaction_cur = self._transaction_conn.cursor()  # Create cursor here
        self._in_transaction = True

    def end_transaction(self, commit: bool = True):
        """
        Ends the transaction by either committing or rolling back all changes.
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

    def in_transaction(self):
        return self._in_transaction

    def _get_connection(self):
        """
        Returns the current connection to be used for operations.
        If a transaction is active, it returns the transaction connection.
        """

        if self._in_transaction:
            return self._transaction_conn
        else:
            return self._pool.getconn()

    def _get_cursor(self, connection):
        """
        Returns the current cursor to be used for operations.
        If a transaction is active, it returns the transaction cursor.
        """
        if self._in_transaction:
            return self._transaction_cur
        else:
            return connection.cursor()

    def get_urls(self, batch_size: int) -> List[str]:
        one_day_ago = datetime.now() - timedelta(days=1)
        args = []

        query = f"SELECT url FROM {self._pages_table} WHERE last_crawled_at IS NULL"
        if self._queued_counter % 5 == 0:
            query += f" OR last_crawled_at < %s"
            args.append(one_day_ago)

        query += " ORDER BY id LIMIT %s;"
        args.append(batch_size)

        # Get connection and cursor manually
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute(query, tuple(args))
        rows = cursor.fetchall()

        # If not in a transaction, commit and release the connection
        if not self._in_transaction:
            connection.commit()
            cursor.close()  # Close the cursor first
            self._pool.putconn(connection)  # Release the connection back to pool

        return [r[0] for r in rows]

    def set_urls_as_queued(self, urls: List[str]) -> None:
        """
        Mark url(s) as queued in the backend

        Args:
            urls (tuple): The page(s) to mark as queued.
        """
        # Get connection and cursor manually
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute("UPDATE pages set queued='true' WHERE url = ANY(%s)", (urls,))

        # If not in a transaction, commit and release the connection
        if not self._in_transaction:
            connection.commit()
            cursor.close()  # Close the cursor first
            self._pool.putconn(connection)  # Release the connection back to pool
        self._queued_counter += 1

    def release_urls(self, urls: List[str]) -> None:
        query = f"UPDATE {self._pages_table} SET queued = FALSE WHERE url = ANY(%s)"
        # Get connection and cursor manually
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute(query, (urls,))

        # If not in a transaction, commit and release the connection
        if not self._in_transaction:
            connection.commit()
            cursor.close()  # Close the cursor first
            self._pool.putconn(connection)  # Release the connection back to pool

    def insert_pages(self, pages_data: List[Dict]) -> None:
        query = "CALL batch_insert_pages(%s)"
        # Get connection and cursor manually
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute(query, (Jsonb(pages_data),))

        # If not in a transaction, commit and release the connection
        if not self._in_transaction:
            connection.commit()
            cursor.close()  # Close the cursor first
            self._pool.putconn(connection)  # Release the connection back to pool

    def delete_pages(self, page_urls: List[str]) -> None:
        query = f"DELETE FROM {self._pages_table} WHERE url = ANY(%s)"
        # Get connection and cursor manually
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.execute(query, (page_urls,))

        # If not in a transaction, commit and release the connection
        if not self._in_transaction:
            connection.commit()
            cursor.close()  # Close the cursor first
            self._pool.putconn(connection)  # Release the connection back to pool

    def increment_failed_tries(self, fail_mapping: Dict) -> None:
        args = []
        for k, v in fail_mapping.items():
            args.append((v, k))
        query = f"UPDATE {self._pages_table} SET failed_tries = failed_tries + %s WHERE url = %s"
        # Get connection and cursor manually
        connection = self._get_connection()
        cursor = self._get_cursor(connection)
        cursor.executemany(query, args)

        # If not in a transaction, commit and release the connection
        if not self._in_transaction:
            connection.commit()
            cursor.close()  # Close the cursor first
            self._pool.putconn(connection)  # Release the connection back to pool
