import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops the tables according to the queries mentioned in drop_table_queries method.
    The arguments are cursor and connection to the database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function takes the cursor and connection as arguments and executes creation of tables in database
    as written in the create_table_queries function.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the driver code of the project. This function fetches the configuration credentials from dwh.cfg.
    Moreover, it initializes a connection and cursor to the database and uses the aforementioned functions within itself.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()