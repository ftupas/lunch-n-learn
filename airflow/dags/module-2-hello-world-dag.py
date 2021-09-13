from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1)
}

with DAG(
    dag_id='module-2-hello-world',
    default_args=DEFAULT_ARGS,
    tags=['airflow','lunchnlearn']
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    task_hello_world = BashOperator(
        task_id='task_hello_world',
        bash_command='echo "hello world"'
    )

    end = DummyOperator(
        task_id='end'
    )

    start >> task_hello_world >> end
