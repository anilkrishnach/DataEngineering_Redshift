# Sparkify Data Analysis using Redshift

## Purpose

This project is to analyse the JSON data that resides in S3 bucket and store in a Star Schema using AWS Redshift.

## Schema

### Input Data
The 2 datasets provide the following information:
1. Log Events: The data contains log information of songs played by users in different sessions.

#### Sample Data
![Log Data](/images/Log_data.png)

2. Song Data: The data contains information regarding individual songs.

The data is given in the form of JSON in 2 S3 buckets. This data is first brought to staging tables in Redshift. They are:
- Staging_Events: For Log data.
- Staging_Songs: For Song data.

#### Sample Data
![Song Data](/images/Song_Data.png)

### Star Schema

The data is now stored in a star schema of 4 dimensional tables and a fact table.
- Fact Table
    - SongPlays
- Dimensional Tables
    - Users
    - Artists
    - Songs
    - Time

![Star Schema](/images/star_schema.png)

## Results - Usage of Redshift

### Staging

The data from S3 buckets is brought into Redshift staging tables by usage of COPY command. This is a faster method of loading data from S3 source.
The column names are either picked up by Redshift using __JSONpaths__ file that we give or setting JSON format to __auto__.

As loading into Fact and Dimensional tables requires joining of the staging tables, we use **Sort Key and DistKey** on both the staging tables schema on the join parameters inorder to collocate data on same nodes. As the data is huge, we go with diststyle KEY on the join parameters so that we can query the data faster although loading into tables might take excessive time.


### Loading into star schema

We now use __INSERT INTO SELECT___ query to select appropriate columns from the staging tables and load them directly into the fact and dimensional tables.
