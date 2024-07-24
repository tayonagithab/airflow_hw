import datetime as dt
import os
import sys

# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
path = os.path.expanduser('~/airflow_hw')
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

# Теперь импортируем необходимые модули
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from modules.predict import predict
from modules.pipeline import pipeline

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 7, 21),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "This pipeline"',
        dag=dag,
    )

    pipeline_task = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
        dag=dag,
    )

    prediction = PythonOperator(
        task_id='Prediction',
        python_callable=predict,
        dag=dag,
    )

    first_task >> pipeline_task >> prediction