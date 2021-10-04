from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import string
import random

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1)
}

def create_random_word(n: int, **kwargs):
    """Creates a random word with length n
    """
    ti = kwargs['ti']
    random_word = ''.join(random.choices(string.ascii_lowercase, k=n))
    print(random_word)
    ti.xcom_push(key='random_word', value=random_word)
    

def remove_vowels(**kwargs):
    """Removes vowels from a word
    """
    ti = kwargs['ti']
    vowels = ['a', 'e', 'i', 'o', 'u']
    word = ti.xcom_pull(key='random_word', task_ids='create_random_word')
    word_no_vowels = ''.join([l for l in word if l not in vowels])
    print(word_no_vowels)
    ti.xcom_push(key='word_no_vowels', value=word_no_vowels)
    

def print_length(**kwargs) -> None:
    """Prints the length of the word
    """
    ti = kwargs['ti']
    word = ti.xcom_pull(key='word_no_vowels', task_ids='remove_vowels')
    print(len(word))

with DAG(
    dag_id='module-4-xcom',
    default_args=DEFAULT_ARGS,
    tags=['airflow','lunchnlearn']
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )

    task_create_random_word = PythonOperator(
        task_id='create_random_word',
        python_callable=create_random_word,
        op_kwargs={'n': 10}
    )

    task_remove_vowels = PythonOperator(
        task_id='remove_vowels',
        python_callable=remove_vowels
    )

    task_print_length = PythonOperator(
        task_id='print_length',
        python_callable=print_length
    )

    start >> task_create_random_word >> task_remove_vowels >> task_print_length >> end
