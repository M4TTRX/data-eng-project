import airflow
import datetime
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.subdags.subdag import subdag
from ingestion_dag import ingestion_dag

args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


default_args_dict = args

global_dag = DAG(
    dag_id='global_dag',
    default_args=default_args_dict,
    catchup=False,
)

# Operator definition
# ===================


start = DummyOperator(
    task_id='start',
    dag=global_dag,
)

ingestion_processus = SubDagOperator(
    task_id='section-1',
    subdag=ingestion_dag('global_dag', 'section-1', args),
    dag=global_dag,
)


end = DummyOperator(
    task_id='end',
    dag=global_dag,
)

start >> ingestion_processus >> end