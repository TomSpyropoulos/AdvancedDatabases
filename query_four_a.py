from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, to_date, unix_timestamp, from_unixtime, count, round, desc
from pyspark.sql.types import FloatType
from geopy.distance import geodesic

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Load the CSV file
df = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
# Load the second CSV file
df2 = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
# Merge the two DataFrames
crime_df = df.union(df2)
# Extract the year
stations_df = spark.read.csv('datasets/LAPD_Police_Stations.csv', header=True, inferSchema=True)


# Filter out records referring to Null Island
crime_df = crime_df.filter((crime_df['LAT'] != 0) & (crime_df['LON'] != 0))

# Filter data for gun-related crimes
crime_df = crime_df.filter(crime_df['Weapon Used Cd'].startswith('1'))

# Join crime data with police stations data
df = crime_df.join(stations_df, crime_df['AREA '] == stations_df['PREC'])

# Define a UDF to calculate distance
def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).km

calculate_distance_udf = spark.udf.register('calculate_distance', calculate_distance, FloatType())

# Calculate distance
df = df.withColumn('distance', calculate_distance_udf(df['LAT'], df['LON'], df['Y'], df['X']))

# Convert the 'DATE_OCC' column to a date type
df = df.withColumn('Date', to_date(from_unixtime(unix_timestamp(df['Date Rptd'], 'dd/MM/yyyy hh:mm:ss a'))))
# Calculate average distance per year
df = df.withColumn('year', year(df['Date']))
# Filter out NULL values of 'year'
df = df.filter(df['year'].isNotNull())
# Calculate average distance per year and count the number of crimes
result = df.groupBy('year').agg(round(avg('distance'),3).alias('average_distance'), count('*').alias('#'))
# Sort by year
result = result.sort('year')

result.show()
# Calculate average distance per office and count the number of crimes
result = df.groupBy(stations_df['DIVISION']).agg((round(avg('distance'), 3)).alias('average_distance'), count('*').alias('#'))
# Sort by number of incidents in descending order
result = result.sort(desc('#'))

result.show(1000)
