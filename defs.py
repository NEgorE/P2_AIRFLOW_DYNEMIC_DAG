from airflow.operators.python_operator import PythonOperator
from P2_AIRFLOW_DYNEMIC_DAG.DBCURSORclass import DBCURSORclass as dbc

import datetime as dt

class defs :

    @staticmethod
    def print_log(task_name, task_npp, log_file) :
        c_dt = dt.datetime.now()
        if task_npp == 1 :
            with open(log_file, "w") as o : 
                input_str = "\n" + '-' * 100 + '\n' + str(c_dt) + ': ' + task_name
                o.write(input_str)
        else :
            with open(log_file, "a") as o : 
                input_str = "\n" + str(c_dt) + ': Start ' + task_name
                o.write(input_str)
    
    @staticmethod
    def cr_py_operator2(xml_obj_mem) :
        dag = xml_obj_mem[3]
        task_name = xml_obj_mem[1]
        r_operator = PythonOperator(
            task_id=task_name,
            python_callable = defs.step,
            op_kwargs={'xml_obj_mem': xml_obj_mem},
            dag=dag
            )
        return r_operator

    @staticmethod
    def step(xml_obj_mem) :
        
        task_npp = xml_obj_mem[0]
        task_name = xml_obj_mem[1]
        task_obj_type = xml_obj_mem[2]
        log_file = xml_obj_mem[3].default_args['log_file']

        defs.print_log(task_name, task_npp, log_file)