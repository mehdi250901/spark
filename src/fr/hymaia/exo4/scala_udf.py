from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time
def main():
    spark = SparkSession.builder \
        .appName("mehdi") \
        .config('spark.jars', 'src/resources/exo4/udf.jar') \
        .getOrCreate()

    def addCategoryName(col):
        # on récupère le SparkContext
        sc = spark.sparkContext
        # Via sc._jvm on peut accéder à des fonctions Scala
        add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
        # On retourne un objet colonne avec l'application de notre udf Scala
        return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

    csv_df = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)

    timeA = time.time()
    df = csv_df.withColumn('category_name', addCategoryName(csv_df['category']))
    df.write.csv("resultat_scala_udf.csv", header=True, mode="overwrite")
    timeB = time.time()
    print("le temps d'éxecution est: ",(timeB - timeA))

    spark.stop()

if __name__ == "__main__":
    main()
