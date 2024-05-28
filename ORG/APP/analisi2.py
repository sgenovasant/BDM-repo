from pyspark.sql import SparkSession
from pyspark.sql.functions import year

# Create a Spark session
spark = SparkSession.builder \
    .appName("County analysis") \
    .config("spark.mongodb.read.connection.uri","mongodb://org-mongo-1:27017/mongo%3Alatest?directConnection=true&appName=mongosh+2.2.5")\
    .config("spark.mongodb.write.connection.uri","mongodb://org-mongo-1:27017/mongo%3Alatest?directConnection=true&appName=mongosh+2.2.5")\
    .getOrCreate()

# Path to the CSV file
csv_file = "/data/us-counties.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(csv_file, header=True, inferSchema=True)
df.printSchema()

df.write.format("mongodb") \
    .mode("overwrite") \
    .option("database", "Dataset") \
    .option("collection", "us-counties") \
    .save()

# Extract only the years from the date column
df = df.withColumn('year', year(df['date']))
df.select('year').distinct().show()

# Count the number of counties, states by name, and geoids
num_counties = df.select("county").distinct().count()
num_states = df.select("state").distinct().count()
num_geoids = df.select("geoid").distinct().count()

# Display results
print("Number of Counties:", num_counties)
print("Number of States:", num_states)
print("Number of Geoids:", num_geoids)

# Print a sample of data for a specific county
# Filter by geoid
df_geoid_filtered = df.filter(df['geoid'] == 'USA-36998')

# Print all rows where the geoid is "USA-53061"
df_geoid_filtered.show()

# Close Spark session
spark.stop()