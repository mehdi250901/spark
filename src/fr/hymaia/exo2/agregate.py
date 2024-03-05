import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
    .appName("mehdi") \
    .getOrCreate()
 
    df_clean = spark.read.option("header", "true").parquet("/home/mehdi/spark-handson/data/exo2/output")
    df_grouped = calculate_population_by_departement(df_clean)
    
    df_grouped.write.csv("/home/mehdi/spark-handson/data/exo2/aggregate", mode="overwrite",header=True)

def calculate_population_by_departement(df):

    df_population_by_departement= df.groupBy("departement").agg(f.count("name").alias("nb_people"))

    df_sorted_population = df_population_by_departement.orderBy(f.desc("nb_people"), f.asc("departement"))
    return df_sorted_population

if __name__ == "__main__":
    main()
