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


path = '/home/airflow/airflow/dags/P2_AIRFLOW_DYNEMIC_DAG/'
log_file = f'{path}logfile.log'
xml_file = f'{path}config.xml'
xml_root = ET.parse(xml_file).getroot()
task_npp = 0
task_name = None
task_mess = None
xml_obj_name = None
xml_obj = None 
xml_obj_pr = None


def print_log(task_mess, task_id) :
    c_dt = dt.datetime.now()
    if task_id == 1 :
        with open(log_file, "w") as o : 
            input_str = "\n" + '-' * 100 + '\n' + str(c_dt) + ': ' + task_mess
            o.write(input_str)
    else :
        with open(log_file, "a") as o : 
            input_str = "\n" + str(c_dt) + ': Start ' + task_mess
            o.write(input_str)

def step(task_mess,task_id) :
    print_log(task_mess, task_id)

def cr_py_operator(task_npp, task_name, task_mess) :
    r_operator = PythonOperator(
            task_id=task_name,
            python_callable = step,
            op_kwargs={'task_mess': task_mess , 'task_id' : task_npp},
            dag=dag
            )
    return r_operator


with DAG(dag_id = 'DAG_dynemic_tree', default_args = args, schedule_interval = None) as dag:

    tasks = {}
    trees = []

    xml_obj_pr = xml_root
    for xml_obj in xml_obj_pr :
        xml_obj_name = xml_obj.attrib['name']
        task_npp += 1
        task_name = f'T{task_npp}_{xml_obj_name}'
        task_mess = f'T{task_npp}: {xml_obj_name}'
        tasks[task_name] = cr_py_operator(task_npp, task_name, task_mess)
        task_name_for_next_step = task_name
        xml_obj_pr = xml_obj
        for xml_obj in xml_obj_pr :
            xml_obj_name = xml_obj.attrib["name"].lower()
            task_npp += 1
            task_name = f'T{task_npp}_{xml_obj_name}'
            task_mess = f'T{task_npp}: {xml_obj_name}'
            tasks[task_name] = cr_py_operator(task_npp, task_name, task_mess)
            trees.append(f'{task_name_for_next_step}>{task_name}')
            task_name_for_next_step = task_name
            xml_obj_pr = xml_obj
            for xml_obj in xml_obj_pr :
                xml_obj_name = xml_obj.attrib["name"].lower()
                task_npp += 1
                task_name = f'T{task_npp}_{xml_obj_name}'
                task_mess = f'T{task_npp}: {xml_obj_name}'
                tasks[task_name] = cr_py_operator(task_npp, task_name, task_mess)
                trees.append(f'{task_name_for_next_step}>{task_name}')
                task_name_for_next_step = task_name
                xml_obj_pr = xml_obj
                for xml_obj in xml_obj_pr :
                    xml_obj_name = xml_obj.attrib["name"].lower()
                    task_npp += 1
                    task_name = f'T{task_npp}_{xml_obj_name}'
                    task_mess = f'T{task_npp}: {xml_obj_name}'
                    tasks[task_name] = cr_py_operator(task_npp, task_name, task_mess)
                    trees.append(f'{task_name_for_next_step}>{task_name}')
                    #task_name_for_next_step = task_name
                    #xml_obj_pr = xml_obj

    
    for value_x in trees :
        key_t = value_x[:value_x.find(">")]
        value_t = value_x[value_x.find(">")+1:]
        tasks[key_t].set_downstream(tasks[value_t])
    


            



        

    