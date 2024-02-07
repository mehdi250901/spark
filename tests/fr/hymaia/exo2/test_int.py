import subprocess
import os

def test_integration():
    job1 = "/home/mehdi/spark-handson/src/fr/hymaia/exo2/agregate.py"
    subprocess.run(["spark-submit", job1])


    job2 = "/home/mehdi/spark-handson/src/fr/hymaia/exo2/clean.py"
    subprocess.run(["spark-submit", job2])

    output_csv_path = "/home/mehdi/spark-handson/data/exo2/aggregate"
    assert os.path.exists(output_csv_path), "CSV du job 2 n'a pas été créé."

if __name__ == "__main__":
    test_integration()