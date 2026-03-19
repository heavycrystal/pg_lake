import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


@pytest.fixture()
def execute_in_pgduck(superuser_conn, app_user):
    # Function is now installed by the extension; just grant access to test user
    run_command(
        f"""
        GRANT EXECUTE ON FUNCTION execute_in_pgduck(text) TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""
        REVOKE EXECUTE ON FUNCTION execute_in_pgduck(text) FROM {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_cancellation(pg_conn, duckdb_conn, execute_in_pgduck):

    # Use a list to share data between threads, as thread-safe for simple operations
    exception_info = []

    # Long-running query
    long_running_query = "select execute_in_pgduck($$select sum(generate_series) FROM generate_series(0,999999999999)$$)"

    # This function will run in a separate thread to allow us to cancel the query
    def execute_long_running_query():
        try:
            # Attempt to execute the long-running query
            cur = pg_conn.cursor()
            cur.execute(long_running_query)
        except Exception as e:
            # Store exception info to check in main thread
            exception_info.append(str(e))

    # Start the long-running query in a thread
    query_thread = threading.Thread(target=execute_long_running_query)
    query_thread.start()

    # give a little bit time for the query to actually start running
    time.sleep(0.1)

    # Now, cancel the query
    pg_conn.cancel()

    # Wait for the thread to finish
    query_thread.join()

    # Check if exception was raised and matches expected
    if exception_info:  # List is not empty, meaning exception was caught
        assert (
            "canceling statement due to user request" in exception_info[0]
        ), "Query cancellation did not raise the expected exception."
    else:
        pytest.fail(
            "No exception caught from cancelled query, expected cancellation exception."
        )

    pg_conn.rollback()


# test for many clients canceled at once
def test_query_cancellation_for_multiple_clients(pg_conn, execute_in_pgduck):
    num_clients = 20

    # Long-running query
    long_running_query = "select execute_in_pgduck($$select sum(generate_series) FROM generate_series(0,999999999999)$$)"

    # Function to execute the long-running query for each client
    def execute_long_running_query(conn, exception_info):
        try:
            cur = conn.cursor()
            cur.execute(long_running_query)
        except Exception as e:
            # Store exception info to check in the main thread
            exception_info.append(str(e))

    # Use a list to store exception info for each client
    exception_infos = []

    # Create and start threads for each client
    threads = []
    for _ in range(num_clients):
        conn = open_pg_conn()
        exception_infos.append([])  # Initialize exception info for this client
        thread = threading.Thread(
            target=execute_long_running_query, args=(conn, exception_infos[-1])
        )
        thread.start()
        threads.append((conn, thread))

    # give a little bit time for the query to actually start running
    time.sleep(0.1)

    # Cancel queries for each client
    for conn, _ in threads:
        conn.cancel()
        conn.rollback()

        # make sure we can still run queries successfully
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchall() == [(1,)]

    # Wait for all threads to finish
    for _, thread in threads:
        thread.join()

    # make sure all clients cancelled
    assert len(exception_infos) == num_clients

    # Check exceptions for each client
    for i, exception_info in enumerate(exception_infos):
        if exception_info:
            assert (
                "canceling statement due to user request" in exception_info[0]
            ), f"Query cancellation for client {i+1} did not raise the expected exception."
        else:
            pytest.fail(
                f"No exception caught from cancelled query for client {i+1}, expected cancellation exception."
            )

    # Cleanup
    for conn, _ in threads:
        conn.close()


def test_subtransaction(pg_conn, duckdb_conn, execute_in_pgduck):
    # Use a list to share data between threads, as thread-safe for simple operations
    exception_info = []

    # Create a test function
    run_command(
        f"""
        SAVEPOINT foo;
    """,
        pg_conn,
    )

    # Long-running query
    long_running_query = "select execute_in_pgduck($$select sum(generate_series) FROM generate_series(0,999999999999)$$)"

    # This function will run in a separate thread to allow us to cancel the query
    def execute_long_running_query():
        try:
            # Attempt to execute the long-running query
            cur = pg_conn.cursor()
            cur.execute(long_running_query)
        except Exception as e:
            # Store exception info to check in main thread
            exception_info.append(str(e))

    # Start the long-running query in a thread
    query_thread = threading.Thread(target=execute_long_running_query)
    query_thread.start()

    # give a little bit time for the query to actually start running
    time.sleep(0.1)

    # Now, cancel the query
    pg_conn.cancel()

    # Wait for the thread to finish
    query_thread.join()

    # Check if exception was raised and matches expected
    if exception_info:  # List is not empty, meaning exception was caught
        assert (
            "canceling statement due to user request" in exception_info[0]
        ), "Query cancellation did not raise the expected exception."
    else:
        pytest.fail(
            "No exception caught from cancelled query, expected cancellation exception."
        )

    run_command(
        """
        ROLLBACK TO SAVEPOINT foo;
        SELECT execute_in_pgduck($$SELECT 1$$);
    """,
        pg_conn,
    )

    pg_conn.rollback()
