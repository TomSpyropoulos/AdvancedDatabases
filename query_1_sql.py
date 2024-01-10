from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder\
            .appName('dataframe_query')\
            .config('spark.executor.instances', '4')\
            .getOrCreate()

start_time = time.time()

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
            YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'M/d/yyyy hh:mm:ss a')))) AS year,
            MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'M/d/yyyy hh:mm:ss a')))) AS month,
            COUNT(*) as crime_total,
            ROW_NUMBER() OVER (PARTITION BY YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'M/d/yyyy hh:mm:ss a')))) ORDER BY COUNT(*) DESC) as row_number
        FROM 
            crimes
        WHERE 
            YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'M/d/yyyy hh:mm:ss a')))) IS NOT NULL AND
            MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'M/d/yyyy hh:mm:ss a')))) IS NOT NULL
        GROUP BY 
            year, month
    ) tmp
    WHERE 
        row_number <= 3
    ORDER BY 
        year ASC, crime_total DESC
""")
with open('./outputs/query_1_sql.txt', 'w') as sys.stdout:
    result.show(50)
    end_time = time.time()
    print(f"Execution time with 4 executor(s): {format(end_time - start_time, '.2f')} seconds")

spark.stop()