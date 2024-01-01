from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, when, desc, rank, col, unix_timestamp, from_unixtime
from pyspark.sql.types import IntegerType, DoubleType



# Initialize a SparkSession
spark = SparkSession.builder.appName('dataframe_query').getOrCreate()
# Load the CSV file
df = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
# Convert the 'DATE_OCC' column to a date type
df = df.withColumn('Date', to_date(from_unixtime(unix_timestamp(df['DATE OCC'], 'dd/MM/yyyy hh:mm:ss a'))))
# Extract the year
df = df.withColumn('year', year(df['Date']))
# Filter out crimes without a victim or their descent
df = df.filter(df['Vict Descent'].isNotNull())
# Create a new column with the desired values
df = df.withColumn('Vict Descent', 
                                when(col('Vict Descent') == 'B', 'Black')
                                .when(col('Vict Descent') == 'W', 'White')
                                .when(col('Vict Descent').isin(['H', 'L', 'M']), 'Hispanic/Latin/Mexican')
                                .otherwise('Unknown'))

# Load the reverse geocoding data
revgeo_df = spark.read.csv('datasets/revgecoding.csv', inferSchema=True, header=True)
# Drop duplicates based on LAT and LON
revgeo_df = revgeo_df.dropDuplicates(['LAT', 'LON'])
# Join the two dataframes on the coordinates
df = df.join(revgeo_df, (df['LAT'] == revgeo_df['LAT']) & (df['LON'] == revgeo_df['LON']))

# Load the income data
income_df = spark.read.csv('datasets/income/LA_income_2015.csv', inferSchema=True, header=True)
# Find the top 3 and bottom 3 zip codes by income
top_3_zip_codes = income_df.orderBy(desc('Estimated Median Income')).select('ZIP Code').limit(3)
bottom_3_zip_codes = income_df.orderBy('Estimated Median Income').select('ZIP Code').limit(3)
zip_codes_df = top_3_zip_codes.union(bottom_3_zip_codes)

df = df.join(zip_codes_df, df['ZIPcode'] == zip_codes_df['ZIP Code'])

# Now you can filter the data for the year 2015
df_2015 = df.filter(df['year'] == 2015)

# Group by descent and count the number of victims
df_grouped = df_2015.groupBy('Vict Descent').count()

# Order by the number of victims
df_grouped = df_grouped.orderBy(desc('count'))

# Show the results
df_grouped.show()

# Stop the SparkSession
spark.stop()