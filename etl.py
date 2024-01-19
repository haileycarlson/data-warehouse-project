# Imports necessary libraries
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
        
# Function for loading data into staging tables
def load_staging_tables(cur, conn):
    """
    Runs COPY queries to load data from S3 into staging tables.

    Args:
        cur: psycopg2 cursor object
        conn: pyscopg2 connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
# Function for inserting data into analytics tables
def insert_tables(cur, conn):
    """
    Runs INSERT queries to populate analytics table from staging tables.

    Args:
        cur: pyscopg2 cursor object
        conn: psycopg2 connection object
    """
    for query in insert_table_queries:
        cur.execute(query)       
        conn.commit()

# Main function to run ETL process
def main():
    """
    Main ETL process that connects to the Redshift cluster, loads staging tables, and inserts data into analytics tables.
    """
    # Reads config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load data into staging tables
    load_staging_tables(cur, conn)

    # Insert data into analytics tables
    insert_tables(cur, conn)

    # Close the database connection
    conn.close()

#  Entry point of the script
if __name__ == "__main__":
    main()