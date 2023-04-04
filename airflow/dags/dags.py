import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta

from app.fantasyDataIngestion.scrapeFantasyPros import webScraper

# Setting default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Initializing dag
dag = DAG(
    "fantasyBrosBaseballDag", default_args=default_args, schedule_interval="@daily"
)

# Calling web scraping class from fantasyDataIngestion module
scraper = webScraper()

scrapeFantasyProsData = PythonOperator(
    task_id="scrapeFantasyProsData", python_callable=scraper.asyncEtl, dag=dag
)

# Bash command for dim_players dbt model
loadBaseballPlayers = BashOperator(
    task_id="loadBaseballPlayers",
    bash_command="cd /dbt/fantasyBrosDbt && dbt run --select dim_players --profiles-dir .",
    dag=dag,
)

# Bash command for dim_players_history dbt model
loadBaseballPlayerHistory = BashOperator(
    task_id="loadBaseballPlayerHistory",
    bash_command="cd /dbt/fantasyBrosDbt && dbt run --select dim_players_history --profiles-dir .",
    dag=dag,
)

# Bash command for dim_benchmarks dbt model
baseballPlayerBenchmarks = BashOperator(
    task_id="baseballPlayerBenchmarks",
    bash_command="cd /dbt/fantasyBrosDbt && dbt run --select dim_benchmarks --profiles-dir .",
    dag=dag,
)

# Bash command for fact_valuations dbt model
baseballPlayerValuations = BashOperator(
    task_id="baseballPlayerValuations",
    bash_command="cd /dbt/fantasyBrosDbt && dbt run --select fact_valuations --profiles-dir .",
    dag=dag,
)


# Setting task order for ELT workflow
(
    scrapeFantasyProsData
    >> loadBaseballPlayers
    >> [loadBaseballPlayerHistory, baseballPlayerBenchmarks]
    >> baseballPlayerValuations
)
