from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, desc, rank, col, unix_timestamp, from_unixtime
from pyspark.sql.window import Window
from pyspark import SparkContext

# Initialize a SparkSession
spark = SparkSession.builder.appName('csv_query').getOrCreate()

# Load the CSV file
df = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)

# Load the second CSV file
df2 = spark.read.csv('datasets/Crime_Data_from_2020_to_Present.csv', inferSchema=True, header=True)

# Merge the two DataFrames
df = df.union(df2)

# Convert the 'DATE_OCC' column to a date type
df = df.withColumn('Date', to_date(from_unixtime(unix_timestamp(df['DATE OCC'], 'dd/MM/yyyy hh:mm:ss a'))))

# Extract the year and month
df = df.withColumn('year', year(df['Date'])).withColumn('month', month(df['Date']))

# Group by Year and Month and count the number of crimes
df_grouped = df.groupBy('year', 'month').count()
df_grouped = df_grouped.withColumnRenamed('count', 'crime_total')

# For each year, get the three months with the greatest number of crimes
window = Window.partitionBy('year').orderBy(desc('crime_total'))
top_months = df_grouped.withColumn('#', rank().over(window)).filter(col('#') <= 3)

# Sort the result by Year in ascending order and Month in descending order
top_months = top_months.orderBy('year', desc('crime_total'))

# filter null values
top_months = top_months.filter(col('year').isNotNull())
# Show the result
top_months.show(50)

# Stop the SparkSession
spark.stop()