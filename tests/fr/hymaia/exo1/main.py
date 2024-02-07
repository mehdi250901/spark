from pyspark.sql import SparkSession
def main():
 spark = SparkSession.builder \
 .appName("wordcount") \
 .master("local[*]") \
 .getOrCreate()

 df = spark.read.csv("/home/mehdi/spark-handson/src/resources/exo1/data.csv",  header=True, inferSchema=True) 
 df.show()

