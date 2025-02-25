# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,udf , when
from pyspark.sql.types import StringType, StructType, StructField, IntegerType , FloatType
import re
from pyspark.sql import functions as F


# Initialize spark session
spark = SparkSession\
        .builder\
        .appName("Reciepe analyzer")\
        .getOrCreate()

# COMMAND ----------

# Create the dataframe by reading the csv file
df = spark.read.csv("dbfs:/FileStore/recipes__1_.csv", header=True, inferSchema=True,escape='"', multiLine=True)

# COMMAND ----------

# Filtering the recipes with beef
beef_reciepes = df.filter(col("ingredients").like("%beef%"))

# COMMAND ----------

# User-defined function to extract hours and minutes from PT format
def parse_duration(duration):
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
    hours = int(match.group(1)) if match and match.group(1) else 0
    minutes = int(match.group(2)) if match and match.group(2) else 0
    seconds = int(match.group(3)) if match and match.group(3) else 0
    total_time = hours * 60 + minutes  + seconds / 60
    return total_time

# Register UDF
parse_duration_udf = udf(parse_duration, FloatType())

# Apply UDF to transform cooking_time column to second
beef_reciepes = beef_reciepes.withColumn("parsed_cooking_time", parse_duration_udf(col("cooking_time")))
beef_reciepes.show()

# COMMAND ----------

# Add ddifficulty lebel for each reciepe on the basis of parsed_cooking_time
beef_reciepes_analysis = beef_reciepes.withColumn(
"difficulty_lebel",
    F.when(col("parsed_cooking_time") < 30.0, "Easy")
    .when((col("parsed_cooking_time") >= 30.0) & (col("parsed_cooking_time") < 60.0), "Medium")
    .otherwise("Hard")
)

# COMMAND ----------

beef_reciepes_analysis.show()

# COMMAND ----------

# Group by difficulty level and calculate the average cooking time
average_cooking_time = beef_reciepes_analysis.groupBy("difficulty_lebel") \
    .agg(F.avg("parsed_cooking_time").alias("avg_cooking_time"))

average_cooking_time.show()
