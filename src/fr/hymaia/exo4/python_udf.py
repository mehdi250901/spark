from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import time

def main():
    spark = SparkSession.builder \
        .appName("mehdi") \
        .getOrCreate()

    csv_df = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)

    def ajout_col(col):
        colonne = "food" if col < 6 else "furniture"
        return colonne

    timeA = time.time()

    ajout_col_UDF = udf(ajout_col, StringType())

    df = csv_df.withColumn('category_name', ajout_col_UDF(csv_df['category']))
    df.write.csv("resultat_python_udf.csv", header=True, mode="overwrite")
    timeB = time.time()
    print("le temps d'éxecution est: ",(timeB - timeA))

    spark.stop()

if __name__ == "__main__":
    main()
