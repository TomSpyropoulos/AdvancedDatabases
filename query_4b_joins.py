from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, min
from pyspark.sql.types import DoubleType
from geopy.distance import geodesic
import sys

method = sys.argv[1]
spark = SparkSession.builder\
            .appName(f"query_4b_{method}_join")\
            .getOrCreate()

# Load datasets
stations = spark.read.csv('/datasets/LAPD_Police_Stations.csv', header=True, inferSchema=True)
crimes = spark.read.csv('/datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('/datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)

def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).km

udf_calculate_distance = udf(calculate_distance, DoubleType())


cross = crimes.crossJoin(stations)
cross = cross.withColumn('distance', udf_calculate_distance(cross['LAT'], cross['LON'], cross['Y'], cross['X']))
nearest_stations = cross.groupBy('DR_NO').agg(min('distance').alias('min_distance'))
crimes_joined = crimes.join(nearest_stations.hint(method), crimes['DR_NO'] == nearest_stations['DR_NO'])

with open(f'./outputs/joins/query_4b_1st_join_{method}.txt', 'w') as f:
    old_stdout = sys.stdout
    sys.stdout = f
    crimes_joined.explain()
    sys.stdout = old_stdout
    crimes_joined.show()

crimes_joined = crimes_joined.join(stations.hint(method), crimes_joined['AREA '] == stations['PREC'])

with open(f'./outputs/joins/query_4b_2nd_join_{method}.txt', 'w') as f:
    old_stdout = sys.stdout
    sys.stdout = f
    crimes_joined.explain()
    sys.stdout = old_stdout
    crimes_joined.show()

spark.stop()