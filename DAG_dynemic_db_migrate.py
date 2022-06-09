from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from P2_AIRFLOW_DYNEMIC_DAG.defs import defs

import datetime as dt
import xml.etree.ElementTree as ET


args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 2, 11),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes = 1),
    'depends_on_past': False,
}

path = '/home/airflow/airflow/dags/P2_AIRFLOW_DYNEMIC_DAG/'
log_file = f'{path}logfile.log'
xml_file = f'{path}config.xml'
xml_root = ET.parse(xml_file).getroot()

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
        cur_sourse = sourse.attrib['name'].lower()
        task_npp += 1
        xml_obj_mem[cur_sourse] = (task_npp , cur_sourse, sourse.tag, log_file, dag)
        tasks[f'T{xml_obj_mem[cur_sourse][0]}_{xml_obj_mem[cur_sourse][1]}'] = defs.cr_py_operator2(xml_obj_mem[cur_sourse])

        for db in sourse :
            cur_db = db.attrib["name"].lower()

            task_npp += 1
            task_name = f'T{task_npp}_{cur_db}'
            task_mess = f'T{task_npp}: {cur_db}'
            tasks[task_name] = defs.cr_py_operator(task_npp, task_name, dag, log_file)

            xml_obj_mem[cur_db] = (task_npp , task_name, task_mess)

            trees.append(f'T{xml_obj_mem[cur_sourse][0]}_{xml_obj_mem[cur_sourse][1]}>{task_name}')

            for schema in db :
                cur_schema = schema.attrib['name'].lower()
                task_npp += 1
                task_name = f'T{task_npp}_{cur_schema}'
                task_mess = f'T{task_npp}: {cur_schema}'
                tasks[task_name] = defs.cr_py_operator(task_npp, task_name, dag, log_file)

                xml_obj_mem[cur_schema] = (task_npp , task_name, task_mess)

                trees.append(f'{xml_obj_mem[cur_db][1]}>{task_name}')

                for table in schema :
                    cur_table = table.attrib['name'].lower()
                    task_npp += 1
                    task_name = f'T{task_npp}_{cur_table}'
                    task_mess = f'T{task_npp}: {cur_table}'
                    tasks[task_name] = defs.cr_py_operator(task_npp, task_name, dag, log_file)

                    xml_obj_mem[cur_table] = (task_npp , task_name, task_mess)

                    trees.append(f'{xml_obj_mem[cur_schema][1]}>{task_name}')

                    task_npp += 1
                    task_name = f'T{task_npp}_{cur_table}'
                    task_mess = f'T{task_npp}: {cur_table}'
                    tasks[task_name] = defs.cr_py_operator(task_npp, task_name, dag, log_file)

                    trees.append(f'{xml_obj_mem[cur_table][1]}>{task_name}')


    
    for value_x in trees :
        key_t = value_x[:value_x.find(">")]
        value_t = value_x[value_x.find(">")+1:]
        tasks[key_t].set_downstream(tasks[value_t])
    


            



        

    