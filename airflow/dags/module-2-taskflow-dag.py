from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from random import randrange

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': True,
    'start_date': days_ago(2)
}

@dag(default_args=DEFAULT_ARGS, tags=['airflow','lunchnlearn'])
def module_2_taskflow():

    @task()
    def generate_consecutive_int() -> list:
        """Generate integers from 0 to random number less than 20
        """
        consecutive_list = [x for x in range(randrange(20))]
        return consecutive_list

    @task()
    def filter_even(consecutive_list: list) -> list:
        """Filter even numbers only
        """
        even_list = list(filter(lambda x: x%2==0, consecutive_list))
        return even_list

    @task()
    def print_length(even_list: list) -> int:
        """Print length of list
        """
        print(f"There is/are {len(even_list)} even number/s.")
    
    consecutive_list = generate_consecutive_int()
    even_list = filter_even(consecutive_list)
    print_length(even_list)

tutorial_dag = module_2_taskflow()
