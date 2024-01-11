from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when, to_timestamp
import sys, time

spark = SparkSession.builder\
            .config('spark.executor.instances', '4')\
            .getOrCreate()

start_time = time.time()
# Merge CSV files to create whole dataset, adjust types
crimes = spark.read.csv('/datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('/datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)
crimes = crimes.withColumn("Hour", (col("TIME OCC") / 100).cast('integer'))

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

with open('./outputs/query_2.txt', 'w') as sys.stdout:
    result.show()
    end_time = time.time()
    print(f"Execution time with 4 executor(s): {format(end_time - start_time, '.2f')} seconds")

spark.stop()