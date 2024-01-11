from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.getOrCreate()

# Merge CSV files to create whole dataset, adjust types, filter
stations = spark.read.csv('/datasets/LAPD_Police_Stations.csv', header=True, inferSchema=True)

crimes = spark.read.csv('/datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('/datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)

join_methods = ["broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]

for method in join_methods:
    crimes_joined = crimes.join(stations.hint(method), crimes['AREA '] == stations['PREC'])

    with open(f'./outputs/joins/query_4a_{method}.txt', 'w') as sys.stdout:
        crimes_joined.explain()
    crimes_joined.show()