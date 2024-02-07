import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
 spark = SparkSession.builder \
 .appName("wordcount") \
 .master("local[*]") \
 .getOrCreate()

 df = spark.read.csv("/home/mehdi/spark-handson/src/resources/exo1/data.csv",  header=True, inferSchema=True) 
 df.show()
 result_df = wordcount(df, "text")
 result_df.write.mode("overwrite").partitionBy('count').parquet("data/exo1/output")
 
 parquet_data = spark.read.parquet("data/exo1/output")
 parquet_data.show()



def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
