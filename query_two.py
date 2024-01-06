from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when, to_timestamp
import sys

spark = SparkSession.builder.appName('dataframe_query').getOrCreate()

# Merge CSV files to create whole dataset, adjust types
crimes = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)
crimes = crimes.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a'))
crimes = crimes.withColumn("Hour", hour(crimes["DATE OCC"]))

#Query
crimes = crimes.withColumn(
    "part_of_day",
    when((col("hour") >= 5) & (col("hour") < 12), "Πρωί")
    .when((col("hour") >= 12) & (col("hour") < 17), "Απόγευμα")
    .when((col("hour") >= 17) & (col("hour") < 21), "Βράδυ")
    .otherwise("Νύχτα")
)

street_crimes = crimes.filter(crimes["Premis Desc"] == "STREET")
result = street_crimes.groupBy("part_of_day").count().orderBy(col("count").desc())

with open('./outputs/query_two.txt', 'w') as sys.stdout:
    result.show()

spark.stop()