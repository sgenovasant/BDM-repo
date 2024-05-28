##"For each state, calculate the total number of cases and deaths per month
##and display the month with the highest number of cases and the month with
#the highest number of deaths for each state and each year."

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import year, month, sum, row_number, col

# Create Spark session and load file
def load_data(MyApp,file_path):
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

# Function to extract year and month from date
def extract_year_month(df):
    # Add year and month columns to the DataFrame
    covid_data = df.withColumn("year", year("date")).withColumn("month", month("date"))
    return covid_data

# Compute total cases and deaths by state and month
def monthly_total_value(covid_data):
    # Group the data by state, year, and month, and sum cases and deaths
    grouped_data = covid_data.groupBy("state", "year", "month") \
        .agg(sum("cases").alias("total_cases"), sum("deaths").alias("total_deaths")) \
        .orderBy("state", "year", "month")
    return grouped_data

# Show the result

def filter_by_cases(grouped_data):
    # Define a window partitioned by state and year, and ordered by total_cases descending
    windowSpec = Window.partitionBy("state", "year").orderBy(col("total_cases").desc())

    # Assign row numbers to each row based on the defined window
    ranked_data = grouped_data.withColumn("row_number", row_number().over(windowSpec))

    # Filter rows where row_number equals 1 (indicating the maximum cases for each state and year)
    max_cases_per_year = ranked_data.filter(col("row_number") == 1)

    # Select the required columns
    max_cases_per_year = max_cases_per_year.select("state", "year", "month", "total_cases")

    return max_cases_per_year

def filter_by_death(grouped_data):
    # Define a window partitioned by state and year, and ordered by total_deaths descending
    windowSpec = Window.partitionBy("state", "year").orderBy(col("total_deaths").desc())

    # Assign row numbers to each row based on the defined window
    ranked_data = grouped_data.withColumn("row_number", row_number().over(windowSpec))

    # Filter rows where row_number equals 1 (indicating the maximum deaths for each state and year)
    max_deaths_per_year = ranked_data.filter(col("row_number") == 1)

    # Select the required columns
    max_deaths_per_year = max_deaths_per_year.select("state", "year", "month", "total_deaths")

    return max_deaths_per_year

def write_to_mongodb(df, collection_name):
    # Write DataFrame to MongoDB
    df.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "Query2") \
        .option("collection", collection_name) \
        .save()

# Main function
def main():
    # Path to the CSV file
    file_path = "/data/us-counties.csv"
    MyApp = "Query2_Spark"
    
    # Load data
    df, spark = load_data(MyApp,file_path)
    
    # Extract year and month from date
    covid_data = extract_year_month(df)
    
    # Calculate total cases and deaths
    grouped_data = monthly_total_value(covid_data)

    # Filter maximum cases per year
    max_cases_per_year = filter_by_cases(grouped_data)
    max_cases_per_year.show(20)

    # Filter maximum deaths per year
    max_deaths_per_year = filter_by_death(grouped_data)
    max_deaths_per_year.show(20)

    #EXPORT MONGODB
    write_to_mongodb(max_cases_per_year, "max_cases_per_year")
    write_to_mongodb(max_deaths_per_year, "max_deaths_per_year")

    # Stop the Spark session
    spark.stop()

# Execute the main function if the script is run directly
if __name__ == "__main__":
    main()