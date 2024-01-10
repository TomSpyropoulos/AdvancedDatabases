from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import IntegerType, DoubleType
import sys

spark = SparkSession.builder.appName('dataframe_query').getOrCreate()

# Merge CSV files to create whole dataset
crimes = spark.read.csv('/datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('/datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)

# Adjust the data types
crimes = crimes.withColumn("Date Rptd", to_date(col("Date Rptd"), 'MM/dd/yyyy'))
crimes = crimes.withColumn("DATE OCC", to_date(col("DATE OCC"), 'MM/dd/yyyy'))
crimes = crimes.withColumn("Vict Age", crimes["Vict Age"].cast(IntegerType()))
crimes = crimes.withColumn("LAT", crimes["LAT"].cast(DoubleType()))
crimes = crimes.withColumn("LON", crimes["LON"].cast(DoubleType()))

with open('./outputs/req_1.txt', 'w') as sys.stdout:
    print("Total number of rows: ", crimes.count())
    crimes.printSchema()

spark.stop()
