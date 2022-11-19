"""
RND DAG for report system

Reads config to determine wich plots to make
Creating plot images
Sends images with an email
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta, datetime

from wwbel.cmn_package.crt_img import create_img
from wwbel.cmn_package.send_mail import send_mail


default_arguments = {
    'owner': 'wwbel',
    'start_date': datetime(2022, 11, 19),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


with DAG(
        "img_crt_send",
        default_args=default_arguments,
        schedule_interval='@daily',
        catchup=False
) as dag:
    task_prepare_dir = BashOperator(
        task_id='prepareDir',
        bash_command='rm -rf /tmp/airflow-images/ && mkdir /tmp/airflow-images/'
    )
    task_create_img = PythonOperator(
        task_id='createImg',
        python_callable=create_img,
        do_xcom_push=True
    )
    task_create_img2 = PythonOperator(
        task_id='createImg2',
        python_callable=create_img,
        op_kwargs={'dots': 5},
        do_xcom_push=True
    )
    task_send_mail = PythonOperator(
        task_id='sendMail',
        python_callable=send_mail,
        op_kwargs={
            'send_from': 'airflow',
            'send_to': ['ww.bel@ya.ru'],
            'subject': 'test from airflow',
            'text': """
                Hello!
                
                Sending images:
                    - {{ task_instance.xcom_pull(task_ids='createImg') }}
                    - {{ task_instance.xcom_pull(task_ids='createImg2') }}
                --
                Sincerely, your Airflow
                """
        }
    )
    task_remove_dir = BashOperator(
        task_id='removeDir',
        bash_command='rm -rf /tmp/airflow-images/',
        trigger_rule=TriggerRule.ALL_DONE
    )
    task_prepare_dir >> [task_create_img, task_create_img2] >> task_send_mail >> task_remove_dir

