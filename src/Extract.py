# Databricks notebook source
import re
import requests
from bs4 import BeautifulSoup


def extract_from_html_content(html: str) -> None:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    results = []
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            record = []
            for cell in cells:
                cell_html = str(cell)
                cell_text = re.search(r'<a href=".*?">(.*?)</a>', cell_html)
                record.append(cell_text.group(1) if cell_text else cell.text.strip())
            results.append(record)

    df = pd.DataFrame(results)
    # Extract the first row as column names
    col_names = df.iloc[0]
    df.columns = col_names
    # Remove the first row from the DataFrame
    df = df[1:] 
    return df

def extract(url: str) -> None:
    response = requests.get(url)
    html_content = response.text
    return extract_from_html_content(html_content)

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(filename='/extract_error.log', level=logging.ERROR)

try:
    spark = SparkSession.builder.appName("Extract_data").getOrCreate()
    nba_link = r'https://www.basketball-reference.com/leagues/NBA_2024_per_game.html#per_game_stats'
    data = extract(nba_link)
    column_names = [name.replace('%', 'Perc') if name else 'ID' for name in data.columns]

    # replace the start of the string if its a number
    column_names_raw = [f"{name[1:]}{name[0]}" if name[0].isdigit() else 
                        f"{name}" for name in column_names]

    data.columns = column_names_raw
    data = data[data['Rk']!='Rk']

    # Define variables used in the code below
    delta_path = "hive_metastore/default/delta/path"
    checkpoint_path = "/checkpoint/path"

    # Convert pandas dataframe to PySpark dataframe
    df = spark.createDataFrame(data)
    
    # Transform Str columns to appropriate types
    int_cols = ['Age', 'G', 'GS']
    float_cols = ['MP', 'FG', 'FGA', 'FGPerc', 'P3', 'PA3', 'PPerc3', 'P2', 'PA2',
                  'PPerc2', 'eFGPerc', 'FT', 'FTA', 'FTPerc', 'ORB', 'DRB', 'TRB',
                  'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']
     
    for column_name in int_cols:
        df = df.withColumn(column_name, df[column_name].cast(IntegerType()))
    
    for column_name in float_cols:
        df = df.withColumn(column_name, df[column_name].cast(DoubleType()))
    
    df.write.mode("overwrite").format("delta").saveAsTable("NBA_PLAYERS_Data_23")
except Exception as e:
    logging.error(str(e))
    print(e)

# COMMAND ----------

data

# COMMAND ----------

delta_table_name = "nba_players_table_3"
spark.sql(f"DROP TABLE IF EXISTS {delta_table_name}")
