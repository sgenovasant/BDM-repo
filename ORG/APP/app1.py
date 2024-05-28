##For each county, compute the daily increment/decrement
##in the average number of COVID-19 cases and deaths,
##as well as the same averages adjusted per 100,000 people, on a certain date

from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col, round, when, row_number
from pyspark.sql.window import Window
import pandas as pd
import time


def load_data(MyApp, file_path):
    # Create a Spark session
    spark = SparkSession.builder \
        .appName(MyApp) \
        .master("spark://org-spark-master-1:7077") \
        .config("spark.mongodb.read.connection.uri","mongodb://org-mongo-1:27017/mongo%3Alatest?directConnection=true&appName=mongosh+2.2.5")\
        .config("spark.mongodb.write.connection.uri","mongodb://org-mongo-1:27017/mongo%3Alatest?directConnection=true&appName=mongosh+2.2.5")\
        .getOrCreate()

    # Load the CSV file
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    return df, spark

def calculate_differences(df):
    # Order the DataFrame by 'geoid' and 'date'
    window = Window.partitionBy('geoid').orderBy('date')

    # Calculate differences between current and previous values for each 'geoid'
    df = df.withColumn('deaths_avg_diff', round(col('deaths_avg') - lag('deaths_avg', 1).over(window), 3)) \
        .withColumn('deaths_avg_per_100k_diff', round(col('deaths_avg_per_100k') - lag('deaths_avg_per_100k', 1).over(window), 3)) \
        .withColumn('cases_avg_diff', round(col('cases_avg') - lag('cases_avg', 1).over(window), 3)) \
        .withColumn('cases_avg_per_100k_diff', round(col('cases_avg_per_100k') - lag('cases_avg_per_100k', 1).over(window), 3))

    # Set the first values of average difference
    df = df.withColumn('cases_avg_diff', when(row_number().over(window) == 1, col('cases_avg')).otherwise(col('cases_avg_diff'))) \
        .withColumn('cases_avg_per_100k_diff', when(row_number().over(window) == 1, col('cases_avg_per_100k')).otherwise(col('cases_avg_per_100k_diff'))) \
        .withColumn('deaths_avg_diff', when(row_number().over(window) == 1, col('deaths_avg')).otherwise(col('deaths_avg_diff'))) \
        .withColumn('deaths_avg_per_100k_diff', when(row_number().over(window) == 1, col('deaths_avg_per_100k')).otherwise(col('deaths_avg_per_100k_diff')))

    return df

def filter_by_date_range(df, start_date, end_date):
    # Convert start and end dates to date format
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Filter the DataFrame by date
    filtered_df = df.filter((col('date') >= start_date) & (col('date') <= end_date))

    return filtered_df

def show_select(df):
    # Show selected columns from the result
    df_new = df.select('date', 'geoid', 'cases_avg', 'cases_avg_diff', 'cases_avg_per_100k_diff')

    return df_new

def write_to_mongodb(df, collection_name):
    # Write DataFrame to MongoDB
    df.write.format("mongodb") \
        .mode("append") \
        .option("database", "Query1") \
        .option("collection", collection_name) \
        .save()

def main():
    # Path to the CSV file
    file_path = "/data/us-counties.csv"
    MyApp = "Query1_Spark"

    # Load data
    df, spark = load_data(MyApp, file_path)

    #  #PROCESS
    # Calculate differences
    df_data = calculate_differences(df)

    # Filter by date range
    start_date = '2020-03-30'
    end_date = '2020-04-15'
    filtered_df = filter_by_date_range(df_data, start_date, end_date)

    # Show result
    df_result = show_select(filtered_df)
    df_result.show(40)

    #  #EXPORT MONGODB
    write_to_mongodb(df_result, "Average_Difference")

    # Stop Spark session
    spark.stop()

# Execute the main function if the script is run directly
if __name__ == "__main__":
    main()