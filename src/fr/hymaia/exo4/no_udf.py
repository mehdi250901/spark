from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import time
def main():
    spark = SparkSession.builder \
        .appName("mehdi") \
        .getOrCreate()

    
    csv_df = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)

    timeA = time.time()
    df = csv_df.withColumn('category_name', f.when(csv_df['category'] < 6, "food").otherwise("furniture"))
    df.write.csv("resultat_no_udf.csv", header=True, mode="overwrite")
    timeB = time.time()
    print("le temps d'éxecution est: ",(timeB - timeA))
    #df.show()
    
    
    window_spec = Window.partitionBy("date","category")
    colonne = f.sum("price").over(window_spec)
    df_new = df.withColumn("total_price_per_category_per_day", colonne)


    #df_new.show()


    window_spec_2 = Window.partitionBy("category").orderBy(f.col("date").desc()).rowsBetween(-29,0)
    colonne2 = f.sum("price").over(window_spec_2)
    df_new_2 = df.withColumn("total_price_per_category_per_day_last_30_days",colonne2)
    #df_new_2.show()

    spark.stop()



if __name__ == "__main__":
    main()
