import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
    .appName("mehdi") \
    .master("local[*]") \
    .getOrCreate()

    city_zipcode = spark.read.csv("/home/mehdi/spark-handson/src/resources/exo2/city_zipcode.csv",  header=True, inferSchema=True) 
    clients_bdd = spark.read.csv("/home/mehdi/spark-handson/src/resources/exo2/clients_bdd.csv",  header=True, inferSchema=True) 
    clients_bdd=clients_bdd.dropna()
    city_zipcode = city_zipcode.dropna()
    df_sans_doublons = city_zipcode.dropDuplicates(["zip"])
    clients_bdd = clients_bdd.dropDuplicates()


    client = filtrer(clients_bdd)

    df_j = Joindre(client,df_sans_doublons)

    df_dep = departement_col_add(df_j)

    df_dep.write.parquet("/home/mehdi/spark-handson/data/exo2/output", mode="overwrite")
    
    
def filtrer(df):
    return df.where(f.col("age")>=18) 

def Joindre(df_clients, df_villes):
    joined_df = df_clients.join(df_villes, "zip" , "left")
    reordered_columns = ["name", "age", "zip", "city"]
    result_df = joined_df.select(*reordered_columns)
    return result_df

def departement_col_add(df):
    return df.withColumn("departement", f.when(
                                                f.substring(f.col("zip"), 1, 2) != "20", f.substring(f.col("zip"), 1, 2)  )
                                                .otherwise(f.when(f.col("zip") <= 20190, "2A").otherwise("2B"))
                                                .alias("departement")
                        )

if __name__ == "__main__":
    main()
