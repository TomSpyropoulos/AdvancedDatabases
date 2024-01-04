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

# Convert the DataFrame to an RDD
rdd = df.rdd

# Define a function to categorize the part of the day
def part_of_day(hour):
    if 5 <= hour < 12:
        return 'Πρωί'
    elif 12 <= hour < 17:
        return 'Απόγευμα'
    elif 17 <= hour < 21:
        return 'Βράδυ'
    else:
        return 'Νύχτα'

# Map the RDD to a pair RDD where the key is the part of the day and the value is 1
pair_rdd = rdd.map(lambda row: (part_of_day(row['DATE OCC'].hour), 1) if row['Premis Desc'] == 'STREET' else None)

# Filter out None values
filtered_rdd = pair_rdd.filter(lambda x: x is not None)

# Reduce by key to count the number of crimes for each part of the day
result_rdd = filtered_rdd.reduceByKey(lambda a, b: a + b)

# Sort by the count in descending order
sorted_rdd = result_rdd.sortBy(lambda x: x[1], ascending=False)

# Collect the result
result = sorted_rdd.collect()

# Print the result
for part_of_day, count in result:
    print(f'{part_of_day}: {count}')