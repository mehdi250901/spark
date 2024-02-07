import unittest 
from pyspark.sql import SparkSession
from pyspark.sql import Row
#from src.fr.hymaia.exo2.agregate.agregate import filtrer, Joindre, departement_col_add 
#from src.fr.hymaia.exo2.clean.clean import calculate_population_by_departement
#from src.fr.hymaia.exo2.spark_clean_job import filtrer, Joindre, departement_col_add,calculate_population_by_departement
#from tests.fr.hymaia.exo2.SparkSession import spark
#from tests.fr.hymaia.spark_test_case import spark


from pyspark.sql import functions as f

from tests.fr.hymaia.spark_test_case import spark 
import sys

from src.fr.hymaia.exo2.clean import filtrer, Joindre, departement_col_add
from src.fr.hymaia.exo2.agregate import calculate_population_by_departement


class SparkScriptTest(unittest.TestCase):
    #spark = SparkSession.builder.master("local[*]").getOrCreate()

    def test_filtrer(self):
        # Given
        data = [Row(name="John", age=25), Row(name="Jane", age=17), Row(name="Doe", age=30)]
        df = spark.createDataFrame(data)

        # When
        actual_df = filtrer(df)

        # Then
        expected_data = [Row(name="John", age=25), Row(name="Doe", age=30)]
        expected_df = spark.createDataFrame(expected_data)
        self.assertEqual(actual_df.collect(), expected_df.collect())
    
    def test_Joindre(self):
        # Given
        data = [Row(name="Cussac", age=27, zip=75020), Row(name="Jane", age=17, zip=75019), Row(name="Doe", age=30, zip=75018)]
        data_villes = [Row(zip=75020, city="Paris"), Row(zip=75019, city="Paris"), Row(zip=75018, city="Paris")]
        df = spark.createDataFrame(data)
        df1 = spark.createDataFrame(data_villes)

        # When
        actual_df = Joindre(df1,df)
        
        # Then
        expected_data = [Row(name="Cussac", age=27, zip=75020, city="Paris"), Row(name="Jane", age=17, zip=75019, city="Paris"), Row(name="Doe", age=30, zip=75018, city="Paris")]
        expected_df = spark.createDataFrame(expected_data)
        self.assertEqual(actual_df.collect(), expected_df.collect())

    def test_departement_col_add(self):
        # Given
        data = [Row(name="Cussac", age=27, zip=75020, city="Paris"), Row(name="Jane", age=17, zip=20190, city="Corse"), Row(name="Doe", age=30, zip=20200, city="Corse")]
        df = spark.createDataFrame(data)
        

        # When
        actual_df = departement_col_add(df)
        
        # Then
        expected_data = [Row(name="Cussac", age=27, zip=75020, city="Paris",departement='75'), Row(name="Jane", age=17, zip=20190, city="Corse",departement='2A'), Row(name="Doe", age=30, zip=20200, city="Corse",departement='2B')]
        expected_df = spark.createDataFrame(expected_data)
        self.assertEqual(actual_df.collect(), expected_df.collect())

    def test_grouper(self):
        # Given
        data = [Row(name="Cussac", age=27, zip=75020, city="Paris",departement='75'), 
                Row(name="Jane", age=17, zip=20200, city="Basita",departement='2B'), 
                Row(name="Doe", age=30, zip=75008, city="Paris",departement='75'), 
                Row(name="Toto", age= 45, zip=33120, city="Arcachon",departement='33')]
        df = spark.createDataFrame(data)
        

        # When
        actual_df = calculate_population_by_departement(df)
        
        # Then
        expected_data = [Row(departement='75',nb_people=2), Row(departement='2B',nb_people=1), Row(departement='33',nb_people=1)]
        expected_df = spark.createDataFrame(expected_data)
        self.assertEqual(actual_df.collect(), expected_df.collect())    

if __name__ == "__main__":
    unittest.main()
