# Databricks notebook source
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

query = "SELECT * FROM prepared_nba_data"
query_result = spark.sql(query)
prepared_nba_data = query_result.toPandas()

prepared_nba_data.head()

# COMMAND ----------

# Create barplot of team rankings
sns.barplot(y=prepared_nba_data.Team_Name, x=prepared_nba_data.Team_Rank)
plt.title('Custom Team Rankings')
plt.xlabel('Rank')
plt.ylabel('Team')
plt.show()

# COMMAND ----------

import numpy as np

# Create a copy of the data and remove the team name column
data = prepared_nba_data.copy()
data = data.drop('Team_Name', axis=1)
data = data.drop('Team_Rank', axis=1)

# Normalize the data by category
data = (data - data.mean()) / data.std()

# Set up the plot
categories = list(data.columns)
teams = list(prepared_nba_data['Team_Name'].values)
x = np.arange(len(categories))
y = np.arange(len(teams))
X, Y = np.meshgrid(x, y)

# Create the plot
plt.figure(figsize=(10, 8))
plt.imshow(data, cmap='viridis', interpolation='nearest')
plt.colorbar(label='Relative Difference')
plt.xticks(x, categories, rotation=90)
plt.yticks(y, teams)
plt.xlabel('Categories')
plt.ylabel('Teams')
plt.title('NBA Team Rankings by Category (Normalized)')
plt.show()
