from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number

# Initialize a SparkSession
spark = SparkSession.builder.appName('csv_query').getOrCreate()

# Load the CSV file
df = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)

# Load the second CSV file
df2 = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)

# Merge the two DataFrames
df = df.union(df2)

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("crime_data")

# Execute SQL query
result = spark.sql("""
    SELECT 
        year, 
        month, 
        crime_total
    FROM (
        SELECT 
            YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'dd/MM/yyyy hh:mm:ss a')))) AS year,
            MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'dd/MM/yyyy hh:mm:ss a')))) AS month,
            COUNT(*) as crime_total,
            ROW_NUMBER() OVER (PARTITION BY YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'dd/MM/yyyy hh:mm:ss a')))) ORDER BY COUNT(*) DESC) as row_number
        FROM 
            crime_data
        WHERE 
            YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'dd/MM/yyyy hh:mm:ss a')))) IS NOT NULL AND
            MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'dd/MM/yyyy hh:mm:ss a')))) IS NOT NULL
        GROUP BY 
            year, month
    ) tmp
    WHERE 
        row_number <= 3
    ORDER BY 
        year ASC, crime_total DESC
""")

result.show(50)

#close spark session
spark.stop()