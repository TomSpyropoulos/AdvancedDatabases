from pyspark.sql.functions import hour, when, count, col
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, date_format
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import from_unixtime, unix_timestamp


# Initialize a SparkSession
spark = SparkSession.builder.appName('dataframe_query').getOrCreate()

# Load the CSV file
df = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)

# Load the second CSV file
df2 = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)

# Merge the two DataFrames
df = df.union(df2)

# Convert the "DATE OCC" column to a 24-hour format timestamp
df = df.withColumn("DATE OCC", from_unixtime(unix_timestamp(df["DATE OCC"], "MM/dd/yyyy hh:mm:ss a"), "yyyy-MM-dd HH:mm:ss"))

# Extract the hour from the timestamp
df = df.withColumn("hour", hour(df["DATE OCC"]))

# Create a new column for the part of the day
df = df.withColumn(
    "part_of_day",
    when((col("hour") >= 5) & (col("hour") < 12), "Πρωί")
    .when((col("hour") >= 12) & (col("hour") < 17), "Απόγευμα")
    .when((col("hour") >= 17) & (col("hour") < 21), "Βράδυ")
    .otherwise("Νύχτα")
)
# Filter for crimes that took place on the street
df_street = df.filter(df["Premis Desc"] == "STREET")

# Group by the part of the day and count the number of crimes
result = df_street.groupBy("part_of_day").count().orderBy(col("count").desc())

# Show the result
result.show()