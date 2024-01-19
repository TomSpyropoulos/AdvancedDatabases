from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, to_date, unix_timestamp, from_unixtime, count, round, desc
from pyspark.sql.types import FloatType
from geopy.distance import geodesic
import sys

spark = SparkSession.builder.getOrCreate()

# Merge CSV files to create whole dataset, adjust types, filter
stations = spark.read.csv('/datasets/LAPD_Police_Stations.csv', header=True, inferSchema=True)

crimes = spark.read.csv('/datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('/datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)
crimes = crimes.withColumn('Date', to_date(from_unixtime(unix_timestamp(crimes['Date Rptd'], 'M/d/yyyy hh:mm:ss a'))))
crimes = crimes.withColumn('year', year(crimes['Date']))
crimes = crimes.filter(crimes['year'].isNotNull())
crimes = crimes.filter((crimes['LAT'] != 0) & (crimes['LON'] != 0))
crimes = crimes.filter(crimes['Weapon Used Cd'].startswith('1')) # Only gun-related


crimes = crimes.join(stations, crimes['AREA '] == stations['PREC'])

def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).km

calculate_distance_udf = spark.udf.register('calculate_distance', calculate_distance, FloatType())

# Query a
crimes = crimes.withColumn('distance', calculate_distance_udf(crimes['LAT'], crimes['LON'], crimes['Y'], crimes['X']))
result = crimes.groupBy('year').agg(round(avg('distance'),3).alias('average_distance'), count('*').alias('#'))
result = result.sort('year')
with open('./outputs/query_4_aa.txt', 'w') as sys.stdout:
    result.show()

# Query b
result = crimes.groupBy(stations['DIVISION']).agg((round(avg('distance'), 3)).alias('average_distance'), count('*').alias('#'))
result = result.sort(desc('#'))
with open('./outputs/query_4_ab.txt', 'w') as sys.stdout:
    result.show(1000)

spark.stop()