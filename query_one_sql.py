from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName('csv_query').getOrCreate()

crimes = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)
crimes.createOrReplaceTempView("crimes")

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
            crimes
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
with open('./outputs/query_one_sql.txt', 'w') as sys.stdout:
    result.show(50)

spark.stop()