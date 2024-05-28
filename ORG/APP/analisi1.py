from pyspark.sql import SparkSession

def load_data(MyApp, file_path):
    # Create a Spark session
    spark = SparkSession.builder \
        .appName(MyApp) \
        .config("spark.mongodb.read.connection.uri","mongodb://org-mongo-1:27017/mongo%3Alatest?directConnection=true&appName=mongosh+2.2.5")\
        .config("spark.mongodb.write.connection.uri","mongodb://org-mongo-1:27017/mongo%3Alatest?directConnection=true&appName=mongosh+2.2.5")\
        .getOrCreate()
    
    # Load the CSV file
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    return df, spark

def usage_column(df):
    # Print the distinct values of each column
    df.select("region_code").distinct().show()
    df.select("days").distinct().show()
    df.select("year").distinct().show()
    # Show all values of a column without truncation
    countries = df.select("country").distinct()
    countries.show(countries.count(), truncate=False)

def conteo_variables(df):
    num_country = df.select("country").distinct().count()
    num_region = df.select("region").distinct().count()
    num_reg_cod = df.select("region_code").distinct().count()
    num_week = df.select("week").distinct().count()

    # Display results
    print("Number of Countries:", num_country)
    print("Number of Regions:", num_region)
    print("Number of Region Codes:", num_reg_cod)
    print("Number of Weeks:", num_week)

def write_to_mongodb(df, collection_name):
    # Write DataFrame to MongoDB
    df.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "Dataset") \
        .option("collection", collection_name) \
        .save()

def main():
    # Path to the CSV file
    file_path = "/data/all_weekly_excess_deaths.csv"
    MyApp = "Region Analysis"

    df, spark = load_data(MyApp, file_path)
    write_to_mongodb(df,"all_weekly_excess_deaths")

    # Display schema
    df.printSchema()

    # Column utility
    usage_column(df)

    conteo_variables(df)

    # Close Spark session
    spark.stop()

if __name__ == "__main__":
    main()