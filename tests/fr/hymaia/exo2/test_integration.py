import unittest
from pyspark.sql import SparkSession
from tests.fr.hymaia.spark_test_case import spark 
from pyspark.sql.functions import count, desc, asc
import os

from src.fr.hymaia.exo2.clean import filtrer, Joindre, departement_col_add
from src.fr.hymaia.exo2.agregate import calculate_population_by_departement

class IntegrationTest(unittest.TestCase):
    def test_integration(self):
        
        file1_path="/home/mehdi/spark-handson/src/resources/exo2/clients_bdd.csv"
        file2_path="/home/mehdi/spark-handson/src/resources/exo2/city_zipcode.csv"       
        
        df1 = spark.read.csv(file1_path, header=True, inferSchema=True)
        df2 = spark.read.csv(file2_path, header=True, inferSchema=True)
       
        df1_filtered = filtrer(df1)
       
        df_joined = Joindre(df1_filtered, df2)
        
        df_with_department = departement_col_add(df_joined)
       
        output_path_first_job = "/home/mehdi/spark-handson/data/exo2/output"

   
        df_with_department.write.mode("overwrite").parquet(output_path_first_job)
        
        input_path_second_job = output_path_first_job
        
        df_clean_with_department = spark.read.parquet(input_path_second_job)
        
        df_population_by_department = calculate_population_by_departement(df_clean_with_department)
        
        output_path_second_job = "/home/mehdi/spark-handson/data/exo2/aggregate"
        
        df_population_by_department.write.csv(output_path_second_job, header=True, mode="overwrite")
         
        self.assertTrue(os.path.exists(output_path_first_job))
        self.assertTrue(os.path.exists(output_path_second_job))


if __name__ == "__main__":
    unittest.main()