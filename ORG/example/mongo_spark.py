#import pymongo
from pyspark.sql import SparkSession

# Inicia una sesión de Spark
spark = SparkSession.builder \
    .appName("MongoDB Spark Connector Example") \
    .master("spark://172.19.0.4:7077") \
    .config("spark.mongodb.read.connection.uri","mongodb://org-mongo-1:27017/mongo%3Alatest?directConnection=true&appName=mongosh+2.2.5")\
    .config("spark.mongodb.write.connection.uri","mongodb://org-mongo-1:27017/mongo%3Alatest?directConnection=true&appName=mongosh+2.2.5")\
    .getOrCreate()

####################################################

df = spark.read.format("mongodb")\
    .option("database","mydatabase")\
    .option("collection","customers")\
    .load()
# Lee datos de MongoDB en un DataFrame de Spark
filtered_df = df.filter(df.address == "Park Lane 38")
filtered_df.show()

# filtrar datos por dirección
#filtered_df = df.filter(df.address == "Park Lane 38")

# Muestra los resultados
#filtered_df.show()

# escribir datos en MongoDB desde Spark
# escribir el DataFrame en MongoDB

filtered_df.write.format("mongodb")\
    .mode("Overwrite")\
    .option("database","mydatabase")\
    .option("collection","customers1")\
    .save()

df1 = spark.read.format("mongodb")\
    .option("database","mydatabase")\
    .option("collection","customers1")\
    .load()

#    .mode("append")\
# Cierra la sesión de Spark
spark.stop()
