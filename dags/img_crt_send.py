"""
RND DAG for report system

Reads config to determine wich plots to make
Creating plot images
Sends images with an email
"""
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from PIL import Image
import numpy as np

import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

default_arguments = {
    'owner': 'wwbel',
    'start_date': datetime(2022, 11, 19),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


def create_img(img_folder='/tmp/airflow-images/', dots=30):
    a = np.random.rand(dots, dots, dots)
    img = Image.fromarray(a, mode='RGB')
    img_path = f'{img_folder}/img_{dots}_{datetime.now().strftime("%Y%m%d")}.png'
    img.save(img_path)
    return img_path


def send_mail(send_from, send_to, subject, text, img_dir='/tmp/airflow-images', server="127.0.0.1"):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = '; '.join(send_to)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in os.listdir(img_dir) or []:
        with open(f'{img_dir}/{f}', "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


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
        op_kwargs={'dots': 40},
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
                    - {{ task_instance.xcom_pull(task_ids='task_create_img') }}
                    - {{ task_instance.xcom_pull(task_ids='task_create_img2') }}
                --
                Sincerely, your Airflow
                """
        }
    )
    task_prepare_dir >> [task_create_img, task_create_img2] >> task_send_mail

