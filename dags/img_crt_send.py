"""
RND DAG for report system

Reads config to determine wich plots to make
Creating plot images
Sends images with an email
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from PIL import Image
import numpy as np

default_arguments = {
    'owner': 'wwbel',
    'start_date': datetime(2022, 11, 19),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def create_img(dots = 300):
    a = np.random.rand(dots, dots, dots)
    img = Image.fromarray(a, mode='RGB')
    return np.asarray(img)


with DAG(
        "img_crt_send",
        default_args=default_arguments,
        schedule_interval='@daily',
        catchup=False
) as dag:
    task_create_img = PythonOperator(
        task_id='createImg',
        python_callable=create_img,
        do_xcom_push=True
    )