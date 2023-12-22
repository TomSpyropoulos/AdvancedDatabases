from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, date_format, from_unixtime, unix_timestamp, hour, when, count, to_timestamp
from pyspark.sql.types import IntegerType, DoubleType


# Initialize a SparkSession
spark = SparkSession.builder.appName('dataframe_query').getOrCreate()
# Load the CSV file
df = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
# Load the second CSV file
df2 = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
# Merge the two DataFrames
df = df.union(df2)
# Convert the "DATE OCC" column to a timestamp
df = df.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a'))
# Extract the hour from the "Timestamp" column and create a new column "Hour"
df = df.withColumn("Hour", hour(df["DATE OCC"]))

print("----------- Exploratory Data Analysis ---------")
# Count the number of rows in the DataFrame
total_count = df.count()
print("Number of rows: ",total_count)
# Filter the DataFrame for rows where "DATE OCC" is null
df_null_date_occ = df.filter(col("DATE OCC").isNull())
# Count the number of rows
null_count = df_null_date_occ.count()
print("Number of null values in 'DATE OCC':", null_count)
# Filter the DataFrame for rows where "Premis Desc" is "STREET"
df_street = df.filter(df["Premis Desc"] == "STREET")
# Count the number of rows
street_count = df_street.count()
print("Number of 'STREET' values in 'Premis Desc': ", street_count)
print("Number of rows expected the result to return:", street_count - null_count)
# Filter the DataFrame for rows where "Hour" is greater than 0
df_hour_greater_than_zero = df.filter(df["Hour"] > 0)
# Count the number of rows
cnt = df_hour_greater_than_zero.count()
print("The rows where the time in DATA OCC is different from 12 AM is:",cnt)

# Create a new column for the part of the day
df = df.withColumn(
    "part_of_day",
    when((df["Hour"] == 0), "Νύχτα")
    .otherwise("Πρωί")
)

# Filter for crimes that took place on the street
df_street = df.filter(df["Premis Desc"] == "STREET")

# Group by the part of the day and count the number of crimes
result = df_street.groupBy("part_of_day").count().orderBy(col("count").desc())

# Show the result
result.show()
# Stop the SparkSession
spark.stop()