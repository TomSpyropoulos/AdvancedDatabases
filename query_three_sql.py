from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName('dataframe_query').getOrCreate()

incomes = spark.read.csv('datasets/income/LA_income_2015.csv', inferSchema=True, header=True)
crimes = spark.read.csv('datasets/Crime_Data_from_2010_to_2019.csv', inferSchema=True, header=True)
revgecoding = spark.read.csv('datasets/revgecoding.csv', inferSchema=True, header=True)

# Register the DataFrames as SQL temporary views
crimes.createOrReplaceTempView("crimes")
incomes.createOrReplaceTempView("incomes")
revgecoding.createOrReplaceTempView("revgeocoding")


# Use SQL to perform the operations
df_grouped = spark.sql("""
    WITH zips AS (
        SELECT `ZIP Code` 
        FROM (
            SELECT `ZIP Code` 
            FROM incomes 
            ORDER BY `Estimated Median Income` DESC 
            LIMIT 3
        )
        UNION ALL
        SELECT `ZIP Code` 
        FROM (
            SELECT `ZIP Code` 
            FROM incomes 
            ORDER BY `Estimated Median Income` ASC 
            LIMIT 3
        )
    ),
    crimes AS (
        SELECT *, YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`DATE OCC`, 'dd/MM/yyyy hh:mm:ss a')))) AS year
        FROM crimes 
        WHERE `Vict Descent` IS NOT NULL
    ),
    rev AS(
        SELECT DISTINCT *
        FROM revgeocoding
    )
    SELECT CASE `Vict Descent`
        WHEN 'B' THEN 'Black'
        WHEN 'W' THEN 'White'
        WHEN 'H' THEN 'Hispanic/Latin/Mexican'
        WHEN 'L' THEN 'Hispanic/Latin/Mexican'
        WHEN 'M' THEN 'Hispanic/Latin/Mexican'
        ELSE 'Unknown'
    END as `New Vict Descent`, COUNT(*) as count
    FROM rev
    JOIN crimes ON crimes.LAT = rev.LAT AND crimes.LON = rev.LON
    JOIN zips ON rev.ZIPcode = zips.`ZIP Code`
    WHERE year = 2015
    GROUP BY `New Vict Descent`
    ORDER BY count DESC
""")

with open('./outputs/query_three_sql.txt', 'w') as sys.stdout:
    df_grouped.show()

spark.stop()