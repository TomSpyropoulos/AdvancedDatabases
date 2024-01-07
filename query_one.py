from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, desc, rank, col, unix_timestamp, from_unixtime
from pyspark.sql.window import Window
import sys, time

spark = SparkSession.builder\
            .appName('dataframe_query')\
            .config('spark.executor.instances', '4')\
            .getOrCreate()

start_time = time.time()
# Merge CSV files to create whole dataset, adjust types
crimes = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
temp = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)
crimes = crimes.union(temp)
crimes = crimes.withColumn('Date', to_date(from_unixtime(unix_timestamp(crimes['DATE OCC'], 'dd/MM/yyyy hh:mm:ss a'))))
crimes = crimes.withColumn('year', year(crimes['Date'])).withColumn('month', month(crimes['Date']))

#Query
crimes_grouped = crimes.groupBy('year', 'month').count()
crimes_grouped = crimes_grouped.withColumnRenamed('count', 'crime_total')
window = Window.partitionBy('year').orderBy(desc('crime_total'))
top_months = crimes_grouped.withColumn('#', rank().over(window)).filter(col('#') <= 3)
top_months = top_months.orderBy('year', desc('crime_total'))
top_months = top_months.filter(col('year').isNotNull())

with open('./outputs/query_one.txt', 'w') as sys.stdout:
    top_months.show(50)
    end_time = time.time()
    print(f"Execution time with 4 executor(s): {format(end_time - start_time, '.2f')} seconds")

spark.stop()