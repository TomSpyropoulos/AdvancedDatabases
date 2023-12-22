from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import IntegerType, DoubleType

# Initialize a SparkSession
spark = SparkSession.builder.appName('dataframe_query').getOrCreate()

# Load the CSV file
df = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)

# Load the second CSV file
df2 = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)

# Merge the two DataFrames
df = df.union(df2)

# Adjust the data types
df = df.withColumn("Date Rptd", to_date(col("Date Rptd"), 'MM/dd/yyyy'))
df = df.withColumn("DATE OCC", to_date(col("DATE OCC"), 'MM/dd/yyyy'))
df = df.withColumn("Vict Age", df["Vict Age"].cast(IntegerType()))
df = df.withColumn("LAT", df["LAT"].cast(DoubleType()))
df = df.withColumn("LON", df["LON"].cast(DoubleType()))

# Print the total number of rows
print("Total number of rows: ", df.count())

# Print the schema
df.printSchema()

# stop spark session
spark.stop()
