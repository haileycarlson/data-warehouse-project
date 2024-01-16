# Project 2: Sparkify Data Warehouse

---------------

## Overview

For this project I was tasked with creating a data warehouse for Sparkify, a startup for music streaming. The purpose of creating a database is to support Sparkify's analytical goals by providing a centralized repository for song related data and user activity. The data warehouse enables Sparkify's data analysts to perform complex queries to gain insight into user behavior, song popularity, and any other relevant metrics.


## Database Schema Design

### Staging Tables
- **staging_events:** Event data from Sparkify app user interactions.
- **staging_songs:** Event data for songs, artists, and other related information.
 
### Analytics Tables
- **songplays:** User interaction records with songs, including details about songs, user, and session.
- **users:** Information on Sparkify users.
- **songs:** Details of songs in the Sparkify music catalog.
- **artists:** Information about the artists of songs.
- **time:** Time-related information extracted from the timestamp of user events. 

## ETL Pipeline

1. **Data Extraction(Extract):** Raw data is extracted from JSON files and stored in Amazon S3 buckets. Two staging tables, `staging_events` and `staging_songs`, are used to temporarily store the data.
2. **Data Transformation (Transform):** Data from staging tables is transformed into the analytics table using SQL queries. Throughout this process data types may be adjusted, and certain transformations, such as timestamp conversions may be used. 
3. **Data Loading (Load):** Transformed data is loaded into the final analytics tables (`songplays`,`users`,`songs`,`artists`,`time`) in the Redshift data warehouse. 

## How to Run the ETL Pipeline

1. Set up Amazon Redshift Cluster.
2. Update the `dwh.cfg` config file with you AWS and Redshift cluster details.
3. Run `create_tables.py` script to create the needed tables in Redshift.
4. Run the `etl.py` script to execute the ETL pipeline, extracting data from S3, transforming it, and loading it into Redshift.

## Dependencies

- Python 3x
- Amazon Redshift cluster
- AWS credentials with necessary permissions
- `boto3` library for interacting with AWS services

## Conclusion

Now, with the use of the data warehouse and ETL pipeline, Sparkify can analyze user behavior and song trends. The schema design allows for efficient querying and reporting, while the ETL process ensures the data is regularly updated to reflect the latest user interactions and song information.