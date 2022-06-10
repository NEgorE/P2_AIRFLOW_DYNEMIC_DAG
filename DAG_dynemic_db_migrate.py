from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from P2_AIRFLOW_DYNEMIC_DAG.defs import defs

import datetime as dt
import xml.etree.ElementTree as ET

path = '/home/airflow/airflow/dags/P2_AIRFLOW_DYNEMIC_DAG/'
log_file = f'{path}logfile.log'
xml_file = f'{path}config.xml'
xml_root = ET.parse(xml_file).getroot()

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 2, 11),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes = 1),
    'depends_on_past': False,
    'log_file' : f'{path}logfile.log'
}

cur_db = None
cur_schema = None
cur_table = None

task_npp = 0
task_name = None
task_mess = None

with DAG(dag_id = 'DAG_dynemic_tree', default_args = args, schedule_interval = None) as dag:

    tasks = {}
    trees = []
    xml_obj_mem = {}


    for sourse in xml_root :
        task_npp += 1
        cur_sourse = f"T{str(task_npp)}_{sourse.attrib['name'].lower()}"
        xml_obj_mem[cur_sourse] = (task_npp, cur_sourse, sourse.tag, dag)
        tasks[cur_sourse] = defs.cr_py_operator2(xml_obj_mem[cur_sourse])

        for db in sourse :
            task_npp += 1
            cur_db = f"T{str(task_npp)}_{db.attrib['name'].lower()}"
            xml_obj_mem[cur_db] = (task_npp, cur_db, db.tag, dag)
            tasks[cur_db] = defs.cr_py_operator2(xml_obj_mem[cur_db])
            trees.append(f'{cur_sourse}>{cur_db}')

            for schema in db :
                task_npp += 1
                cur_schema = f"T{str(task_npp)}_{schema.attrib['name'].lower()}"
                xml_obj_mem[cur_schema] = (task_npp, cur_schema, schema.tag, dag)
                tasks[cur_schema] = defs.cr_py_operator2(xml_obj_mem[cur_schema])
                trees.append(f'{cur_db}>{cur_schema}')

                for table in schema :
                    task_npp += 1
                    cur_table = f"T{str(task_npp)}_{table.attrib['name'].lower()}"
                    xml_obj_mem[cur_table] = (task_npp, cur_table, table.tag, dag)
                    tasks[cur_table] = defs.cr_py_operator2(xml_obj_mem[cur_table])
                    trees.append(f'{cur_schema}>{cur_table}')

                    task_npp += 1
                    cur_table2 = f"T{str(task_npp)}_{table.attrib['name'].lower()}"
                    xml_obj_mem[cur_table2] = (task_npp, cur_table2, table.tag, dag)
                    tasks[cur_table2] = defs.cr_py_operator2(xml_obj_mem[cur_table2])
                    trees.append(f'{cur_table}>{cur_table2}')


    
    for value_x in trees :
        key_t = value_x[:value_x.find(">")]
        value_t = value_x[value_x.find(">")+1:]
        tasks[key_t].set_downstream(tasks[value_t])
    


            



        

    