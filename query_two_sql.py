from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import sys

spark = SparkSession.builder.appName('dataframe_query').getOrCreate()

# Merge CSV files to create whole dataset, adjust types
crimes = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)
crimes = crimes.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))

#Query
crimes.createOrReplaceTempView("crimes")

result = spark.sql("""
    SELECT 
        CASE
            WHEN HOUR(`DATE OCC`) >= 5 AND HOUR(`DATE OCC`) < 12 THEN 'Πρωί'
            WHEN HOUR(`DATE OCC`) >= 12 AND HOUR(`DATE OCC`) < 17 THEN 'Απόγευμα'
            WHEN HOUR(`DATE OCC`) >= 17 AND HOUR(`DATE OCC`) < 21 THEN 'Βράδυ'
            ELSE 'Νύχτα'
        END AS part_of_day,
        COUNT(*) as count
    FROM crimes
    WHERE `Premis Desc` = 'STREET'
    GROUP BY part_of_day
    ORDER BY count DESC
""")
with open('./outputs/query_two_sql.txt', 'w') as sys.stdout:
    result.show()

spark.stop()