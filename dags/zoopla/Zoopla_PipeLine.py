from zoopla_resources import Add_new_records,Comparing_Data,Data_Cleaning,Update_Delete_Records,zoopla_spider
from airflow import DAG
import os
import logging
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta


def get_path_json():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    download_page_folder_path = os.path.join(current_dir, "output","zoopla_listing_data.json")
    return download_page_folder_path
    
def wrapper_add_new_records(**kwargs):
    ti = kwargs['ti']
    new_records = ti.xcom_pull(task_ids='Compare_Data', key='new_records')
    Add_new_records(new_records).Main()
    
def wrapper_update_delete_records(**kwargs):
    ti = kwargs['ti']
    delete_records = ti.xcom_pull(task_ids='Compare_Data', key='delete_records')
    Update_Delete_Records(delete_records).Main()

def run_zoopla_spider(**_):
    """
    Wrapper that starts the Scrapy spider.
    Putting the import INSIDE the function
    keeps DAG-parsing light and lets logs appear.
    """


    logging.warning("===> Zoopla spider is starting")
    zoopla_spider.Main()                         # your real entry-point
    logging.warning("===> Zoopla spider finished")


with DAG('Zoopla_PipeLine',
         default_args={
             'owner': 'Home Portfolio',
             'depends_on_past': False,
             'start_date': datetime(2025, 4, 18),
             'retries': 1,
             'catchup': False,
             'retry_delay': timedelta(minutes=5)
         },
         schedule_interval='@weekly',  # Run every week
         catchup=False) as dag:
    
    #-------------------------------------------
    # [1] Running the spider to scrape data from Zoopla and return the JSON file
    #-------------------------------------------
    run_spider = PythonOperator(task_id = 'Running_Zoopla_Spider',
                                python_callable = run_zoopla_spider)  # No parentheses! for the function becasue to prevent it from running immediately and wait for the scheduler to run it 
    
    
    
    #-------------------------------------------
    # [2] Sensor to wait for the spider to finish and create the JSON file
    #-------------------------------------------
    Wait_Spider_Finish = FileSensor(
        task_id="Wait_Spider_Finish",
        filepath="/root/airflow/include/zoopla_resources/output/zoopla_listing_data.json", 
        poke_interval=30,  # check every 30 seconds
        timeout=259200,       # wait max 3 days
        fs_conn_id = 'fs_default',
        mode="poke"        # or use "reschedule" if your system is big
    )
    Wait_Spider_Finish.doc_md = '''
    sensor for checking if the spider has finished running and json file is created 
    -->> it will check every 30 seconds for 3 days
    '''

    #-------------------------------------------
    # [3] convert the JSON file to CSV file after parsing the data
    #-------------------------------------------
    clean_data = PythonOperator(task_id = 'Clean_Data',
                                python_callable = Data_Cleaning.Main)  
    
    clean_data.doc_md = '''
    Cleaning the data from the JSON file and convert it to CSV file
    '''

    #-------------------------------------------
    # [4] Compare the cleaned data with existing records in ElasticSearch
    #------------------------------------------- 
    compare_data = PythonOperator(task_id='Compare_Data',
                                  python_callable = Comparing_Data.Main)
    
    compare_data.doc_md = '''
    Compare the cleaned data with existing records in ElasticSearch to identify new and deleted records
    '''
    
    #-------------------------------------------
    # [5] Add new records to the database
    #------------------------------------------- 
    add_new_records = PythonOperator(task_id='Add_New_Records',
                                    python_callable =  wrapper_add_new_records)

    add_new_records.doc_md = '''
    add new records to the database using the list of new records obtained from the comparison step
    '''

    #-------------------------------------------
    # [6] Add new records to the database
    #------------------------------------------- 
    update_delete_records = PythonOperator(task_id='Update_Delete_Records',
                                            python_callable = wrapper_update_delete_records)
    update_delete_records.doc_md='''
    update the status of records in ElasticSearch according to the list of IDs provided
    '''

    # Define task dependencies
    run_spider >> Wait_Spider_Finish >> clean_data >> compare_data >> [add_new_records , update_delete_records]