# Databricks End to End Demo
### by Rakeen Rouf

[![PythonCiCd](https://github.com/rmr327/DataBricksEndtoEndDemo/actions/workflows/python_ci_cd.yml/badge.svg)](https://github.com/rmr327/DataBricksEndtoEndDemo/actions/workflows/python_ci_cd.yml)
---

## **Summary**

The project demonstrates an end-to-end data processing pipeline using Databricks notebooks and Azure Databricks workflows. The pipeline walks through an example of ingesting raw data, transforming data, and running analyses on the processed data. The pipeline uses SQL queries to create and insert data into a prepared_song_data table. The table consists of columns such as artist name, duration, release, tempo, and more, and the processed data is stored into this table.

The project also highlights that Databricks recommends using the declarative interface for building reliable, maintainable, and testable data processing pipelines called Delta Live Tables.

Overall, the project serves as a good starting point for those looking to learn about data pipelines and how to implement them in Azure Databricks.

** Done by following instruction guide from data bricks.

---

## **The complete workflow pipeline**

We start by ingesting some songs data, then we transform it and load it into a table, then we perform some analyis on in.

![image](https://github.com/rmr327/DataBricksEndtoEndDemo/assets/36940292/17584460-80e1-4970-99e4-43e863545104)

Examples of succesful data pipeline work flows can be seen below.

![image](https://github.com/rmr327/DataBricksEndtoEndDemo/assets/36940292/3a821c16-c463-41f4-beed-3a2e18f36661)

---

### **Data Ingestion**

The code below creates a streaming DataFrame (Databricks Delta Table) from a folder "/databricks-datasets/songs/data-001/" that contains tab-separated CSV files named "song_data". It specifies a schema for the song_data table which includes fields such as artist_id, song_hotness, title, year, etc. that will be used to define the structure and types of the data to be read.

It uses the cloudFiles format to read files from the cloud, then specifies options for the format to use "csv" and the separator to be "\t".

Finally, it writes the data to a checkpoint location "/tmp/pipeline_get_started/_checkpoint/song_data" and outputs it to a Databricks Delta Table with a name of table_name which in this case is "song_data".

The trigger(availableNow=True) option specifies that the query should be triggerable immediately upon startup of the Stream, without waiting for any data to arrive. It doesn't wait, i.e it will treat empty directories as available for processing.

![image](https://github.com/rmr327/DataBricksEndtoEndDemo/assets/36940292/d9f6eef4-8437-45a8-9954-a1e9ba0144e8)

### **Data Transformation /Preparation**

The code below creates a table named prepared_song_data and inserts data from another table named song_data.

The CREATE TABLE statement specifies the schema for the prepared_song_data table consisting of columns artist_id (string), artist_name (string), duration (double), release (string), tempo (double), time_signature (double), title (string), year (double) and processed_time (timestamp).

Then the INSERT INTO statement fills the prepared_song_data table with data from the song_data table. It is required that the schema of the tables match to avoid errors. The SELECT statement selects data from the song_data table and, instead of the current_timestamp() function, uses the function SYSDATE() to get the current system date and time as timestamp and inserts that timestamp value into the processed_time column of the prepared_song_data table.

This query is written in SQL syntax and executed using Spark's SQL engine, making it a Spark SQL query.

![image](https://github.com/rmr327/DataBricksEndtoEndDemo/assets/36940292/d98839e6-f58d-4193-8939-070b4140562a)

### **Data Analysis**

The SQL code below counts the number of songs by artist per year where year is greater than 0 by grouping the artist name and year fields in prepared_song_data. It returns the result sorted by descending count of songs and year.

This query is written in SQL syntax and executed using Spark's SQL engine, making it a Spark SQL query.

![image](https://github.com/rmr327/DataBricksEndtoEndDemo/assets/36940292/17646df8-246f-4baa-99da-3287a3c958df)

The SQL code below selects artist name, song title and tempo from table prepared_song_data, where Time signature is 4 and tempo is between 100 and 140.

This query is written in SQL syntax and executed using Spark's SQL engine, making it a Spark SQL query.

![image](https://github.com/rmr327/DataBricksEndtoEndDemo/assets/36940292/43e79c31-b39b-4058-838d-b7ca290f1550)

## **Triggering Workflows**

Scheduling workflows is a powerful way to automate data processing and analysis tasks using Databricks. Once a workflow has been created, it can be scheduled to run on a regular basis using various methods.

One method of scheduling workflows is to trigger them based on specific conditions, such as file or data stream arrival times. This can be achieved using Databricks' trigger capabilities, which allow for workflows to be executed based on changes to specific files or data streams. To create a trigger, you can set up a Databricks Trigger which is linked to an Event-based Trigger such as AWS S3 or Azure Data Lake Storage (ADLS).

Another way to schedule workflows is by creating a cron job based on time intervals using Databricks REST API. This involves creating a token with API access permission and generating a personal access token, which can then be used for authentication to call the REST API.

In both cases above, these workflows run within the context of a Databricks job. You may want to consider how often your data source is updating or how fast data is expected to be processed before setting up a schedule.

Therefore, scheduling workflows can help with automating data processing and analysis tasks, as well as help manage large-scale data processing pipelines in a more efficient way.
