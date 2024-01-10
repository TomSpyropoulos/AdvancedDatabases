from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, to_date, unix_timestamp, from_unixtime, count, round, udf, min, desc
from pyspark.sql.types import DoubleType
from geopy.distance import geodesic
import sys

spark = SparkSession.builder.getOrCreate()

# Merge CSV files to create whole dataset, adjust types, filter
stations = spark.read.csv('datasets/LAPD_Police_Stations.csv', header=True, inferSchema=True)

crimes = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)
crimes = crimes.withColumn('Date', to_date(from_unixtime(unix_timestamp(crimes['Date Rptd'], 'M/d/yyyy hh:mm:ss a'))))
crimes = crimes.withColumn('year', year(crimes['Date']))
crimes = crimes.filter(crimes['year'].isNotNull())
crimes = crimes.filter((crimes['LAT'] != 0) & (crimes['LON'] != 0))
crimes = crimes.filter(crimes['Weapon Used Cd'].startswith('1'))


def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).km

udf_calculate_distance = udf(calculate_distance, DoubleType())

cross = crimes.crossJoin(stations)
cross = cross.withColumn('distance', udf_calculate_distance(cross['LAT'], cross['LON'], cross['Y'], cross['X']))
nearest_stations = cross.groupBy('DR_NO').agg(min('distance').alias('min_distance'))
crimes = crimes.join(nearest_stations, crimes['DR_NO'] == nearest_stations['DR_NO'])
crimes = crimes.join(stations, crimes['AREA '] == stations['PREC'])


# Query a
result = crimes.groupBy('year').agg(round(avg('min_distance'),3).alias('average_distance'), count('*').alias('#'))
result = result.sort('year')
with open('./outputs/query_4_ba.txt', 'w') as sys.stdout:
    result.show()

# Query b
result = crimes.groupBy(crimes['DIVISION']).agg((round(avg('min_distance'), 3)).alias('average_distance'), count('*').alias('#'))
result = result.sort(desc('#'))
with open('./outputs/query_4_bb.txt', 'w') as sys.stdout:
    result.show(21)

spark.stop()