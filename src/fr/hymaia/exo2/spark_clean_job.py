from pyspark.sql import SparkSession
import  pyspark.sql.functions as f
from pyspark.sql.functions import broadcast 
import os
def main( param1,param2,param3, spark ):
    
 
    file1_path=param1
 
    file2_path=param2
    

 
    df1 = spark.read.csv(file1_path, header=True, inferSchema=True)

 
    df2 = spark.read.csv(file2_path, header=True, inferSchema=True)
 
    f1_filter=filter1(df1)
   
    #jointure:
    joint= jointure(f1_filter,df2)
   
 
    #ajouter la colonnes departement 
    joint= ajoutColonne(joint)
    
    output_path = param3
    
    joint.coalesce(1).write.mode("overwrite").parquet(output_path)

     

 
def filter1(df1):
    if df1.isEmpty():
        raise ValueError("Le DataFrame est vide, la fonction filter ne peut pas être appliquée.")
    df1_filter=df1.filter(df1["age"]>=18)
    return df1_filter
def jointure(f1_filter, df2):
    # Nettoyer les DataFrames
    df2 = df2.dropDuplicates(["zip"])
    joint = f1_filter.join(df2, "zip","inner")
    reordered_columns = ["name", "age", "zip", "city"]
    joint = joint.select(*reordered_columns)
    return joint
def ajoutColonne(joint):
    result_with_department = joint.withColumn(
    "departement",
    f.when((f.substring(f.col("zip"), 1, 2) == "20") & (f.col("zip") > 20190), "2A")
    .when((f.substring(f.col("zip"), 1, 2) == "20") & (f.col("zip") <= 20190), "2B")
    .otherwise(f.substring(f.col("zip"), 1, 2))
    )
    return result_with_department