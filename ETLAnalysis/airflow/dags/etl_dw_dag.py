from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# IMPORTANTE:
# En el contenedor, tu proyecto se monta en /opt/airflow (por el docker-compose).
PROJECT_DIR = "/opt/airflow"

default_args = {"owner": "airflow"}

with DAG(
    dag_id="etl_dw_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    t_extract = BashOperator(
        task_id="extract",
        bash_command=f"python {PROJECT_DIR}/src/extract.py",
    )

    t_transform = BashOperator(
        task_id="transform",
        bash_command=f"python {PROJECT_DIR}/src/transform.py",
    )

    t_integrate = BashOperator(
        task_id="integrate",
        bash_command=f"python {PROJECT_DIR}/src/integrate.py",
    )

    t_prepare = BashOperator(
        task_id="prepare",
        bash_command=f"python {PROJECT_DIR}/src/prepare.py",
    )

    t_load = BashOperator(
        task_id="load",
        bash_command=f"python {PROJECT_DIR}/src/load.py",
    )

    t_extract >> t_transform >> t_integrate >> t_prepare >> t_load
