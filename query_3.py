from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, when, desc, col, unix_timestamp, from_unixtime
import sys, time

for executors_num in [2,3,4]:

    spark = SparkSession.builder\
                .appName('dataframe_query')\
                .config('spark.executor.instances', str(executors_num))\
                .getOrCreate()

    start_time = time.time()
    # Load datasets, adjust types, filter for null
    incomes = spark.read.csv('datasets/income/LA_income_2015.csv', inferSchema=True, header=True)

    revgecoding = spark.read.csv('datasets/revgecoding.csv', inferSchema=True, header=True)
    revgecoding = revgecoding.dropDuplicates(['LAT', 'LON'])

    crimes = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
    crimes = crimes.withColumn('Date', to_date(from_unixtime(unix_timestamp(crimes['DATE OCC'], 'M/d/yyyy hh:mm:ss a'))))
    crimes = crimes.withColumn('year', year(crimes['Date']))
    crimes = crimes.filter(crimes['Vict Descent'].isNotNull())

    # Adjust values for output
    crimes = crimes.withColumn('Vict Descent', 
                                    when(col('Vict Descent') == 'B', 'Black')
                                    .when(col('Vict Descent') == 'W', 'White')
                                    .when(col('Vict Descent').isin(['H', 'L', 'M']), 'Hispanic/Latin/Mexican')
                                    .otherwise('Unknown'))

    # Query
    crimes = crimes.join(revgecoding, (crimes['LAT'] == revgecoding['LAT']) & (crimes['LON'] == revgecoding['LON']))

    top_zip = incomes.orderBy(desc('Estimated Median Income')).select('ZIP Code').limit(3)
    bottom_zip = incomes.orderBy('Estimated Median Income').select('ZIP Code').limit(3)
    zip_codes_df = top_zip.union(bottom_zip)

    crimes = crimes.join(zip_codes_df, crimes['ZIPcode'] == zip_codes_df['ZIP Code'])
    crimes_2015 = crimes.filter(crimes['year'] == 2015)
    crimes_by_desc = crimes_2015.groupBy('Vict Descent').count()
    crimes_by_desc = crimes_by_desc.orderBy(desc('count'))

    with open(f'./outputs/query_3_exec{executors_num}.txt', 'w') as sys.stdout:
        crimes_by_desc.show()
        end_time = time.time()
        print(f"Execution time with {executors_num} executor(s): {format(end_time - start_time, '.2f')} seconds")

spark.stop()