from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


def extract_data():
    download_url = 'https://raw.githubusercontent.com/Davidooj/Projects/main/AimLab/GridshotUltimate_%20Data.csv'
    output_filename = 'C:/Users/david/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc/LocalState' \
                      '/rootfs/home/davidmartinez/airflow/dags/Gridshot_Ultimate_Data.csv'
    response = requests.get(download_url)

    if response.status_code == 200:
        with open(output_filename, 'wb') as f:
            f.write(response.content)
        print(f"CSV file downloaded to '{output_filename}'")
    else:
        print("Failed to download CSV file")

def transform_data():
    import pandas as pd
    import matplotlib.pyplot as plt

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    df = pd.read_csv(
        r'C:/Users/david/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc/LocalState' \
                      '/rootfs/home/davidmartinez/airflow/dags/Gridshot_Ultimate_Data.csv')
    df2 = df.drop(["accB0", "accB1", "accB2", "accB3", "accB4", "accB5", "accB6", "accB7", 'rtB0', 'rtB1', 'rtB2',
                   'rtB3', 'rtB4', 'rtB5', 'rtB6', 'rtB7', "version", "map", "mode", "weaponName"], axis=1)
    df3 = df2.rename(columns={"targetsTotal": "Total Shots",
                              "shotsTotal": "Hits Per Sec",
                              "killsPerSec": "Total Hits",
                              "accTotal": "Accuracy Rate",
                              "createDate": "Task",
                              "rtTotal": "Reaction Time(ms)",
                              "taskName": "Task Name"})
    df4 = df3.drop(["Task Name", "killTotal"], axis=1)
    df4.iloc[95:, [1, 2]] = df4.iloc[95:, [2, 1]]
    print(df4)

    output_filename = 'C:/Users/david/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc/LocalState' \
                      '/rootfs/home/davidmartinez/airflow/dags/Transformed_Data.csv'
    df4.to_csv(output_filename, index=False)

# Define default_args and create a DAG instance
default_args = {
    'owner': 'David DAG',
    'start_date': datetime(2023, 8, 1),
    'retries': 1
}

dag = DAG(
    'PIPELINE_dag',
    default_args=default_args,
    schedule=None,
)

# Define tasks using PythonOperators
task_extract_data = PythonOperator(
    task_id='task_extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_transform_data = PythonOperator(
    task_id = 'task_transform_data',
    python_callable = transform_data,
    dag=dag,
)

# Task Dependencies
task_extract_data >> task_transform_data