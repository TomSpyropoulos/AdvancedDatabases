from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import sys

spark = SparkSession.builder.appName('dataframe_query').getOrCreate()

# Merge CSV files to create whole dataset, adjust types
crimes = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)
crimes = crimes.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))

#Define fun used for query
rdd = crimes.rdd
def part_of_day(hour):
    if 5 <= hour < 12:
        return 'Πρωί'
    elif 12 <= hour < 17:
        return 'Απόγευμα'
    elif 17 <= hour < 21:
        return 'Βράδυ'
    else:
        return 'Νύχτα'

#Query
pair_rdd = rdd.map(lambda row: (part_of_day(row['DATE OCC'].hour), 1) if row['Premis Desc'] == 'STREET' else None)
filtered_rdd = pair_rdd.filter(lambda x: x is not None)
result_rdd = filtered_rdd.reduceByKey(lambda a, b: a + b)
sorted_rdd = result_rdd.sortBy(lambda x: x[1], ascending=False)
result = sorted_rdd.collect()

with open('./outputs/query_two_rdd.txt', 'w') as sys.stdout:
    for part_of_day, count in result:
        print(f'{part_of_day}: {count}')