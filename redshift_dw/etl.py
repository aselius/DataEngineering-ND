import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies over data from S3 bucket into staging tables for redshift.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into separate fact and dim tables from staging.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST = config.get("CLUSTER", "HOST")
    DBNAME = config.get("CLUSTER", "DB_NAME")
    USER = config.get("CLUSTER", "DB_USER")
    PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    PORT = config.get("CLUSTER", "DB_PORT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST, DBNAME, USER, PASSWORD, PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()