
#Filter the 'n' countries with the highest number of COVID deaths, present in the month with the most COVID deaths,
#for each trimester in the year, also calculate the average COVID deaths per 100k inhabitants

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

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

    return df,spark

def get_data(df):
    # Select the columns of interest
    new_df = df.select("country","start_date","week","population","covid_deaths","covid_deaths_per_100k")
    # Extract the year, and month from "start_date"
    new_df = new_df.withColumn("year", F.year("start_date"))
    new_df = new_df.withColumn("month", F.month("start_date"))
    # Divide the year into trimesters
    new_df = new_df.withColumn("trimester", F.floor((new_df["month"] - 1) / 3) + 1)
    # Filter rows where there is COVID deaths data
    new_df = new_df.filter(df["covid_deaths"] > 0)
    return new_df
      
def sum_death_bymonth(df):
    # Calculate the total COVID-19 deaths for each month
    # Define a window partitioned by year and ordered by month
    windowSpec = Window.partitionBy("year").orderBy("month")

    df_month_total = df.withColumn("monthly_covid_deaths", F.sum("covid_deaths").over(windowSpec))
    return df_month_total

def sum_death_bycountry(df):
    # Calculate the total COVID-19 deaths for each month, in each country
    # Define a window partitioned by year, country and ordered by month 
    windowSpec = Window.partitionBy("year", "country").orderBy(F.col("month"))

    df_country = df.withColumn("country_covid_deaths", F.sum("covid_deaths").over(windowSpec)) \
        .withColumn("avg_deaths_per100k", F.round(F.avg("covid_deaths_per_100k").over(windowSpec), 4))

    return df_country

def max_deaths_month(df):
    # Calculate the maximum monthly COVID deaths per trimester
    # Define a window partitioned by year and order by trimester
    windowSpec = Window.partitionBy("year").orderBy(F.col("trimester"))

    df_max_monthly_deaths = df.withColumn("max_monthly_deaths", F.max("monthly_covid_deaths").over(windowSpec))

    # Filter: keep only the rows with the maximum monthly deaths per trimester
    df_filtered = df_max_monthly_deaths.filter(df_max_monthly_deaths["monthly_covid_deaths"] == df_max_monthly_deaths["max_monthly_deaths"])

    # Select the columns of interest
    new_df = df_filtered.select("country", "year", "trimester", "month", "country_covid_deaths", "avg_deaths_per100k","monthly_covid_deaths")
    new_df = new_df.distinct()

    return new_df

def top_countries(df, n):
    # Filter the top 'n' number of countries with the highest number of deaths in the months with the highest number of COVID-19 deaths, obtained in the max_deaths_month(df) function
    # Define the window for partitioning by year, trimester and ordering deaths in descending order
    window_spec = Window.partitionBy("year", "trimester").orderBy(F.desc("country_covid_deaths"))
    # Assign a rank to country_covid_deaths, ordered within the partition
    df = df.withColumn("rank", F.rank().over(window_spec)).filter(F.col("rank") <= n)
    new_df = df.select("country", "year", "trimester", "month", "country_covid_deaths", "avg_deaths_per100k", "monthly_covid_deaths")
  
    return new_df

def write_to_mongodb(df, collection_name):
    # Write DataFrame to MongoDB
    df.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "Query3") \
        .option("collection", collection_name) \
        .save()

def main():
    # Path to the CSV file
    file_path = "/data/all_weekly_excess_deaths.csv"
    # Application name
    MyApp = "Query3_Spark"
    
    # Call functions
    df, spark = load_data(MyApp, file_path)
    
    df_data = get_data(df)
    df_sum_bymonth = sum_death_bymonth(df_data)
    df_sum_bycountry = sum_death_bycountry(df_sum_bymonth)
    df_filter_max = max_deaths_month(df_sum_bycountry)
    df_query3 = top_countries(df_filter_max,5)
    df_query3.show()

    #EXPORT MONGOBD
    write_to_mongodb(df_filter_max, "Max_deaths_year")
    write_to_mongodb(df_query3, "Countries_Rancking")

    # End Spark session
    spark.stop()

# Execute the main function if the script is run directly
if __name__ == "__main__":
    main()
