# Import necessary libraries 
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Function to drop existing tables
def drop_tables(cur, conn):
    """
    Runs DROP queries to remove existing tables.

    Args:
        cur: psycopg2 cursor object
        conn: psycopg2 connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# Function to create new tables
def create_tables(cur, conn):
    """
    Runs CREATE queries to create new tables.

    Args:
        cur: psycopg2 cursor object
        conn: psycopg2 connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# Main function to execute table creation or deletion
def main():
    """
    Main script to connnect to the Redshift cluster, drop existing tables, and create new tables according to the specified schema.
    """
    # Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop existing tables
    drop_tables(cur, conn)

    # Create new tables
    create_tables(cur, conn)

    # Close the connection to the database
    conn.close()

# Entry point of the script
if __name__ == "__main__":
    main()