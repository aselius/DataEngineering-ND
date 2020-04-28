import boto3
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all current tables.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging tables as well as fact and dim tables.
    """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()