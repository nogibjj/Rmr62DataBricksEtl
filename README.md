# Databricks End-to-End ETL for Custom Nba Team Rankings
### by Rakeen Rouf

[![PythonCiCd](https://github.com/nogibjj/Rmr62DataBricksEtl/actions/workflows/python_ci_cd.yml/badge.svg)](https://github.com/nogibjj/Rmr62DataBricksEtl/actions/workflows/python_ci_cd.yml)
---

## **Summary**

The project demonstrates an end-to-end data processing pipeline using Databricks notebooks and Azure Databricks workflows. The pipeline walks through an example of ingesting raw NBA teams data, transforming the data, and running analyses on the processed data. The Pipiline starts by extracting data from an online table located on the nba reference website. The pipeline uses SQL queries to create and insert data into a prepared_nba_data table. The table consists of columns such as custrom_metric (custom team ranking), Average Points per Player, Average Defensive Rebounds per Player, Average turnover per player, and more, and the processed data is stored into a delta lake table. 


---

## **The complete workflow pipeline**

We start by ingesting some songs data, then we transform it and load it into a table, then we perform some analyis on in.

![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/031d03ce-e94b-4649-85dd-bef4b6e1acaf)

Examples of succesful data pipeline work flows can be seen below.

![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/93a48fbd-4c78-4532-a654-2cb1d50d3a00)

---

### **Data Extraction**

This notebook extracts data from a table on Basketball Reference website using Python BeautifulSoup package and writes it to a Delta table in Databricks. The pandas DataFrame is cleaned and PySpark DataFrame is created based on the schema definition that specifies the appropriate data types. Finally, PySpark DataFrame is written into a Delta table using the write.format() function.

The function extract_from_html_content() uses BeautifulSoup package to extract the table contents from the HTML source code and transform it to a pandas DataFrame. Then, a schema definition is created, and PySpark DataFrame is created by using the schema definition with the createDataFrame() function.

If an error occurs during the data extraction, an error message will be written to a log file named extract_error.log. The logging module is used to handle potential exceptions and store the error messages.

The implementation showcases how to extract web data into a PySpark DataFrame, transform the data, and save it to a Delta table using Databricks in a single notebook. The logging feature provides a way to log the error messages for easier debugging of the program.

![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/d687f703-9820-46c5-9f50-36816b29e657)
![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/0f6fe11e-1776-447e-bf78-19355abe2103)


### **Data Transformation & Load**

This notebook reads the NBA player data from the previosly specified delta lake table and transforms the data into a form that is suitable for team ranking analysis. This involves computing various statistics for each team, normalizing the statistics using min-max normalization, and ranking the teams based on a custom metric. The results are then stored in another delta lake table in the Databricks environment.

Moreover, this notebook performs data quality checks on the prepared data to ensure that it is valid and suitable for analysis. In particular, it checks for duplicate rows using SQL queries and PySpark commands. Data quality checks are important because they help ensure that the results of the analysis are accurate and reliable. If the data quality checks indicate any problems with the data, further investigation and cleaning may be necessary.

![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/e661418f-1f5a-4e45-a8c4-2d3acf0004c8)
![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/a79db1a7-1ee0-412f-b340-5f9a572da1eb)
![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/1b5eaf17-9df9-4aa0-8c2d-5a820b930399)
![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/75e36175-e67a-4d34-a91f-4379a3be36a9)
![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/ad602bec-34f1-4020-ab2a-ba90ff00d930)

Data validation

![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/0159d216-64e9-4ca9-8c2e-d82940a6b1e6)


### **Data Analysis**

This notebook demonstrates the use of Databricks and several data analysis and visualization tools, such as Spark SQL, Pandas, Matplotlib, and Seaborn, to analyze and visualize NBA team rankings data. The notebook imports data from a prepared Spark delta lake table, converts it to a Pandas DataFrame, and then performs several operations to explore the data and create visualizations. The code blocks include examples of how to retrieve and manipulate data, create bar plots, scatter plots, and heatmaps, normalize data, and rotate x-tick labels.

The graph below shows our custom team rankings for the 2024 NBA season as of Dec 1 2024.
![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/e9970756-bffc-488e-9adc-6a3b43d3d226)


The graph below shows our custom team rankings broken down by each category for the 2024 NBA season as of Dec 1 2024.
![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/ec080cd0-7433-4159-a046-2c460ae773b8)

![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/7d0475cb-685d-46e7-a2ff-5ad5e378dae8)

## **Triggering Workflows**

Scheduling workflows is a powerful way to automate data processing and analysis tasks using Databricks. Once a workflow has been created, it can be scheduled to run on a regular basis using various methods.

One method of scheduling workflows is to trigger them based on specific conditions, such as file or data stream arrival times. This can be achieved using Databricks' trigger capabilities, which allow for workflows to be executed based on changes to specific files or data streams. To create a trigger, you can set up a Databricks Trigger which is linked to an Event-based Trigger such as AWS S3 or Azure Data Lake Storage (ADLS).

Another way to schedule workflows is by creating a cron job based on time intervals using Databricks REST API. This involves creating a token with API access permission and generating a personal access token, which can then be used for authentication to call the REST API.

In both cases above, these workflows run within the context of a Databricks job. You may want to consider how often your data source is updating or how fast data is expected to be processed before setting up a schedule.

Therefore, scheduling workflows can help with automating data processing and analysis tasks, as well as help manage large-scale data processing pipelines in a more efficient way.


`The worflow for this project is triggered to run every Friday.`

![image](https://github.com/nogibjj/Rmr62DataBricksEtl/assets/36940292/8c5f9487-6a76-4ff3-8d93-86ba104ef472)
