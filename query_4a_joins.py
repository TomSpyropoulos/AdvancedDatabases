from pyspark.sql import SparkSession
import sys

method = sys.argv[1]
spark = SparkSession.builder\
            .appName(f"query_3_{method}_join")\
            .getOrCreate()

# Merge CSV files to create whole dataset, adjust types, filter
stations = spark.read.csv('/datasets/LAPD_Police_Stations.csv', header=True, inferSchema=True)

crimes = spark.read.csv('/datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('/datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)


crimes_joined = crimes.join(stations.hint(method), crimes['AREA '] == stations['PREC'])

with open(f'./outputs/joins/query_4a_1st_join_{method}.txt', 'w') as f:
    old_stdout = sys.stdout
    sys.stdout = f
    
    sys.stdout = old_stdout
    crimes_joined.show()

spark.stop()