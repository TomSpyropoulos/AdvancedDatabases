from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import sys

spark = SparkSession.builder.getOrCreate()

# Load datasets
incomes = spark.read.csv('/datasets/income/LA_income_2015.csv', inferSchema=True, header=True)
revgecoding = spark.read.csv('/datasets/revgecoding.csv', inferSchema=True, header=True)
crimes = spark.read.csv('/datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)

# Joins
join_methods = ["broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]

for method in join_methods:
    crimes_joined = crimes.join(revgecoding.hint(method), (crimes['LAT'] == revgecoding['LAT']) & (crimes['LON'] == revgecoding['LON']))
    with open(f'./outputs/joins/query_3_1st_join_{method}.txt', 'w') as sys.stdout:
        crimes_joined.explain()

    top_zip = incomes.orderBy(desc('Estimated Median Income')).select('ZIP Code').limit(3)
    bottom_zip = incomes.orderBy('Estimated Median Income').select('ZIP Code').limit(3)
    zip_codes_df = top_zip.union(bottom_zip)

    crimes_joined = crimes_joined.join(zip_codes_df.hint(method), crimes_joined['ZIPcode'] == zip_codes_df['ZIP Code'])

    with open(f'./outputs/joins/query_3_2nd_join_{method}.txt', 'w') as sys.stdout:
        crimes_joined.explain()
    crimes_joined.show()

spark.stop()