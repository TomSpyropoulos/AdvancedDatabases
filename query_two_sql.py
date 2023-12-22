from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, date_format, from_unixtime, unix_timestamp, hour, when, count, to_timestamp


# Initialize a SparkSession
spark = SparkSession.builder.appName('dataframe_query').getOrCreate()
# Load the CSV file
df = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
# Load the second CSV file
df2 = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
# Merge the two DataFrames
df = df.union(df2)

# Convert the "DATE OCC" column to a timestamp
df = df.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("crimes")

# Execute the SQL query
result = spark.sql("""
    SELECT 
        CASE
            WHEN HOUR(`DATE OCC`) >= 5 AND HOUR(`DATE OCC`) < 12 THEN 'Πρωί'
            WHEN HOUR(`DATE OCC`) >= 12 AND HOUR(`DATE OCC`) < 17 THEN 'Απόγευμα'
            WHEN HOUR(`DATE OCC`) >= 17 AND HOUR(`DATE OCC`) < 21 THEN 'Βράδυ'
            ELSE 'Νύχτα'
        END AS part_of_day,
        COUNT(*) as count
    FROM crimes
    WHERE `Premis Desc` = 'STREET'
    GROUP BY part_of_day
    ORDER BY count DESC
""")

# Show the result
result.show()

# Stop the SparkSession
spark.stop()