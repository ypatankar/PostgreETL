import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    This function creates a new database, sparkifydb and
    then creates cursor and connection objects for other functions to use.

    :return: cur: cursor object for SQL connection
    :return: conn: connection object to connect to the database
    """

    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute("SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();")
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    This function drops tables in sparkifydb database based on the list in sql_queries.py.

    :param cur: Cursor for the SQL connection
    :param conn: Connection object to connect to database
    :return: None
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates tables in sparkifydb database based on the list in sql_queries.py.

    :param cur: Cursor for the SQL connection
    :param conn: Connection object to connect to database
    :return: None
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def create_sequence(cur, conn):
    """
    This function creates a sequence for column songplay_id in songplays.

    :param cur: Cursor for the SQL connection
    :param conn: Connection object to connect to database
    :return: None
    """

    cur.execute("CREATE SEQUENCE sp start 1 increment 1;")
    conn.commit()


def main():
    """
    This function calls other functions for creating database, dropping tables, creating tables and creating sequence
    and passes cursor and connection objects as arguments.

    :return: None
    """

    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    create_sequence(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()