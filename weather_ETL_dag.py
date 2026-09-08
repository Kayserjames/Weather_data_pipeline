from airflow.sdk import DAG, task, get_current_context
from Weather_pipeline_utils import extract, transform, quality_check, load
from datetime import datetime, timedelta
import pendulum  
import pandas as pd 
import os


cities = ["Douala","Yaounde","Buea","Kribi","Garoua","Bertoua",
          "Maroua","Bamenda","Ngaoundere","Bafoussam","Limbe"]


with DAG( 
     dag_id = "Weatherdata_etl",
     start_date= pendulum.datetime(2026, 8, 25, tz="Africa/Douala"),
     schedule="0 14 * * *",
     catchup=False,
) as dag:

    @task(retries=2,
          retry_delay= timedelta(minutes=1))
    def extract_task():
        api_key = os.environ["API_KEY"]
        dataframe = extract(cities= cities, api_key= api_key)

        min_cities = 0.8  # au moins 80% des villes attendues

        n_received = dataframe["city"].nunique() if not dataframe.empty else 0
        n_expected = len(cities)
        completeness = (n_received / n_expected) if n_expected else 0

        if completeness < min_cities:
            raise ValueError(
                f"[extract_task] Seulement {n_received}/{n_expected} villes récupérées "
                f"({completeness:.0%}), en dessous du seuil de {min_cities:.0%} — "
                "nouvelle tentative déclenchée."
            )

        raw_file = "/tmp/raw_file.csv"

        dataframe.to_csv(raw_file, index=False)

        print(f"{len(dataframe)} lines extracted")
        print(f"file created : {raw_file}")

        return raw_file


    @task
    def transform_task(raw_file):
        dataframe = pd.read_csv(raw_file)

        transform_data = transform(extracted_data= dataframe)

        transform_file = "/tmp/transform_file.csv"

        transform_data.to_csv("/tmp/transform_file.csv", index=False)

        print(f"{len(transform_data)} lines transformed")
        print(f"created file : {transform_file}")

        return transform_file


    @task
    def quality_check_task(transform_file):
        dataframe = pd.read_csv(transform_file)
        quality_check(dataframe, expected_cities=cities)

        return transform_file  



    @task(retries=2,
          retry_delay= timedelta(minutes=1))
    def load_task(transform_file):
        dataframe = pd.read_csv(transform_file)
        context = get_current_context()
        data_interval_start = context["data_interval_start"]

        load(transformed_data= dataframe, target_table= "cameroon_cities", 
             Dag_run= data_interval_start.strftime("%Y-%m-%d"))


    raw_file = extract_task()
    transform_file = transform_task(raw_file)
    checked_file = quality_check_task(transform_file)
    load_task(checked_file)

    

