from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

import time as tm
import datetime as dt
import xml.etree.ElementTree as ET

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 2, 11),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes = 1),
    'depends_on_past': False,
}

def step(**kwargs) :
    key1 = kwargs['key1']
    print(f'LOL {key1}')

xml_file = '/home/airflow/airflow/dags/P2_AIRFLOW_DYNEMIC_DAG/config.xml'
xml_root = ET.parse(xml_file).getroot()
task_id = 0

with DAG(dag_id = 'DAG_dynemic_tree', default_args = args, schedule_interval = None) as dag:

    tasks = {}

    for sourse in xml_root :
        cur_sourse = sourse.attrib['name']
        task_id += 1
        tasks[cur_sourse] = PythonOperator(
            task_id = f'T{task_id}_{cur_sourse}',
            python_callable = step,
            op_kwargs={'key1': 'LOLec'},
            dag=dag
            )
        #for db in sourse :
        #    cur_db = db.attrib["name"].lower()
        #    tasks[cur_db] = PythonOperator(
        #        task_id='step1',
        #        python_callable=step1,
        #        op_kwargs={'key1': 'LOL'},
        #        dag=dag
        #    )

        

    