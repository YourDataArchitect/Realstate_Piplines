from include.zoopla_resources import (
                            Comparing_Data,
                            Data_Cleaning,
                            Analyse_Records,
                            Extract_Skipped_Districts)
from airflow import DAG
import os
import logging
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

doc = """
## 📌 Zoopla Property Data Pipeline
This DAG runs **weekly** to extract, clean, compare, and analyze property listings from Zoopla.  
Results are pushed to **Elasticsearch** and **Google Sheets** for reporting.

### 🔄 Pipeline Stages
- **Extract & Clean**: Scrapes new listings, cleans them, and stores in MongoDB[Zoopla.Raw_Data].
- **Compare & Update**: Compares against existing data and updates Elasticsearch:
  - Adds new listings
  - Flags deleted ones
  - Updates common records
- **Analytics**: Generates statistics and feeds them to:
  - stage_4_analysis  in Elasticsearch
  - Google Sheets 

### 📅 Schedule
- Runs:  @weekly  (Every Sunday)

### 📦 Indices Used
-  prod_zoopla_stage_2_clean 
-  prod_zoopla_stage_3_properties_all 
-  prod_zoopla_stage_4_analysis 

![Picture of the ISS](https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTMD17nUoqektiX0JwVGfM704YUL6EOIDkZSg&s)
"""


def get_path_json():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    download_page_folder_path = os.path.join(current_dir, "output","zoopla_listing_data.json")
    return download_page_folder_path
    
def wrapper_add_new_records(**kwargs):
    from include.zoopla_resources import Add_new_records # import separate for minimize import libraries time 
    ti = kwargs['ti']
    new_records = ti.xcom_pull(task_ids='Extract_Data_Cleaning_Validation.Compare_Data', key='new_records')
    Add_new_records.AddNewRecords(new_records).Main()
    
def wrapper_update_delete_records(**kwargs):
    from include.zoopla_resources import Update_Delete_Records # import separate for minimize import libraries time 
    ti = kwargs['ti']
    delete_records = ti.xcom_pull(task_ids='Extract_Data_Cleaning_Validation.Compare_Data', key='delete_records')
    Update_Delete_Records.UpdateStatus(delete_records).Main()
    
def wrapper_update_common_records(**kwargs):
    from include.zoopla_resources import Update_Common_records # import separate for minimize import libraries time 
    ti = kwargs['ti']
    common_records = ti.xcom_pull(task_ids='Extract_Data_Cleaning_Validation.Compare_Data', key='common_records')
    Update_Common_records.update_records(common_records).Main()

def wrapper_Feed_es(**kwargs):
    from include.zoopla_resources import Feed_Analytics_Statistics_ES # import separate for minimize import libraries time 
    ti = kwargs['ti']
    analysis_records = ti.xcom_pull(task_ids='Analyse_Data_Feeding_ES_GS.Analyse_Records', key='analysis_records')
    Feed_Analytics_Statistics_ES.Feeding(analysis_records).Main()

def run_Feeding_GS(**kwargs):
    from include.zoopla_resources import Feeding_google_sheet # import separate for minimize import libraries time 
    "Wrapper that starts Feeding Google Sheet."
    ti = kwargs['ti']
    analysis_records = ti.xcom_pull(task_ids='Analyse_Data_Feeding_ES_GS.Analyse_Records', key='analysis_records')
    Feeding_google_sheet.google_sheet(analysis_records).Main()
    


def run_zoopla_spider(**_):
    from include.zoopla_resources import zoopla_spider
    
    """
    Wrapper that starts the Scrapy spider.
    Putting the import INSIDE the function
    keeps DAG-parsing light and lets logs appear.
    """


    logging.warning("===> Zoopla spider is starting")
    zoopla_spider.Main()                         # your real entry-point
    logging.warning("===> Zoopla spider finished")


def run_extract_skipped_districts(**_):
    "Wrapper that starts extract skipped districts."
    Extract_Skipped_Districts.Main()

def run_delete_mongo(**_):
    from include.zoopla_resources import Delete_Mongo
    
    """
    Wrapper that starts the Scrapy spider.
    Putting the import INSIDE the function
    keeps DAG-parsing light and lets logs appear.
    """
    logging.info("===> Delete Raw_data Mongo Collection")
    Delete_Mongo.main()                       # your real entry-point





with DAG('Zoopla_PipeLine',
        default_args={
            'depends_on_past': False,
            'start_date': datetime(2025, 6, 9),   
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'owner' : 'Home Portfolio'},
        schedule='30 12 * * 0', # For running every sunday at 12:30 pm UTC - every week and every month 
        doc_md=doc, 
        catchup=False) as dag:
    
    with TaskGroup(group_id= 'Extract_Data_Cleaning_Validation') as extract_prepare_data:   
        #-------------------------------------------
        # [1-1] Send Slack alert when the DAG starts
        #-------------------------------------------
        
        # Send Slack alert when the DAG starts
        notify_start = SlackWebhookOperator(
                        task_id="Notify_Start",
                        slack_webhook_conn_id="slack_webhook",
                        message="""
:rocket: DAG Started!
*DAG Name: `{{ dag.dag_id }}`*
*Run ID: `{{ run_id }}`* 
                    """)
        
        
        #-------------------------------------------
        # [1-2] Delete Mongo DB (Old data)
        #-------------------------------------------
        delete_records = PythonOperator(task_id = 'Delete_Old_Records',
                                    python_callable = run_delete_mongo)  # No parentheses! for the function becasue to prevent it from running immediately and wait for the scheduler to run it 
        
        delete_records.doc_md='''
        Delete all old records in "Raw_Data" 
        '''
        
        #-------------------------------------------
        # [1-3] Running the spider to scrape data from Zoopla and return the JSON file
        #-------------------------------------------
        run_spider = PythonOperator(task_id = 'Running_Zoopla_Spider',
                                    python_callable = run_zoopla_spider)  # No parentheses! for the function becasue to prevent it from running immediately and wait for the scheduler to run it 
        
        run_spider.doc_md='''
        Running Zoopla scraper and feeding data into [Raw_Data] Mongodb .
        '''
        
        #-------------------------------------------
        # [1-4] Cleaning data from ES [prod_zoopla_stage_1_raw] and feeding : [prod_zoopla_stage_2_clean]
        #-------------------------------------------
        clean_data = PythonOperator(task_id = 'Cleaning_Data_Validation',
                                    python_callable = Data_Cleaning.Main)  
        
        clean_data.doc_md = '''
        Cleaning the data from MongoDB Collection [Raw_Data] To Elasticsearch [prod_zoopla_stage_2_clean].
        '''

        #-------------------------------------------
        # [1-5] Compare the cleaned data with existing in index [prod_zoopla_stage_2_clean] with [prod_zoopla_stage_3_properities_all]
        #------------------------------------------- 
        compare_data = PythonOperator(task_id='Compare_Data',
                                    python_callable = Comparing_Data.Main)
        
        compare_data.doc_md = '''
        Compare the cleaned data in [prod_zoopla_stage_2_clean] with the existing data in [prod_zoopla_stage_3_properties_all]  
        to identify and extract the following groups:  
        - **new_records**: appear in stage 2 but not in stage 3  
        - **deleted_records**: missing from the latest scrape  
        - **common_records**: found in both indexes
        '''
        
        #-------------------------------------------
        # [1-6] Add new records to the database
        #------------------------------------------- 
        add_new_records = PythonOperator(task_id='Add_New_Records',
                                        python_callable =  wrapper_add_new_records)

        add_new_records.doc_md = '''
        add new records to the database using the list of new records obtained from the comparison step
        '''

        #-------------------------------------------
        # [1-7] Add new records to the database
        #------------------------------------------- 
        update_delete_records = PythonOperator(task_id='Flag_Delete_Records',
                                                python_callable = wrapper_update_delete_records)
        update_delete_records.doc_md='''
        update the status of records in ElasticSearch according to the list of IDs provided
        '''

        #-------------------------------------------
        # [1-8] Update Common records in the database
        #------------------------------------------- 
        Common_update_Records = PythonOperator(task_id='Update_Common_Records',
                                                python_callable = wrapper_update_common_records)
        Common_update_Records.doc_md='''
        update the status of records in ElasticSearch according to the list of IDs provided
        '''
        
        notify_start >> delete_records >> run_spider >> clean_data >> compare_data >> [add_new_records , update_delete_records , Common_update_Records]
    with TaskGroup(group_id= 'Analyse_Data_Feeding_ES_GS') as analyse_data_feeding_GS_ES:
        
        #-------------------------------------------
        # [2-1] Analyse records - 
        #------------------------------------------- nOT fINISHED 
        Analyse_Records_ = PythonOperator(task_id='Analyse_Records',
                                        python_callable = Analyse_Records.Main) 
        Analyse_Records_.doc_md='''
        Analayse the records and push them to XCOM for feeding to ES
        '''
        
        #-------------------------------------------
        # [2-2] Feeding Analytics data to ES [prod_zoopla_stage_4_analysis]
        #------------------------------------------ nOT fINISHED  
        feed_analytics_statistics_to_es = PythonOperator(task_id='Feed_Analytics_Statistics_ES',
                                            python_callable = wrapper_Feed_es) 
        
        feed_analytics_statistics_to_es.doc_md='''
        Feeding analytics data to ES [prod_zoopla_stage_4_analysis]
        '''

        #-------------------------------------------
        # [2-3] Feeding analytics data to Google Sheet
        #------------------------------------------ nOT fINISHED 
        feed_analytics_statistics_to_google_sheet = PythonOperator(task_id='Feed_Analytics_Statistics_Google_Sheet',
                                            python_callable = run_Feeding_GS) #>>>>>>>>>>
        feed_analytics_statistics_to_google_sheet.doc_md='''
        update the status of records in ElasticSearch according to the list of IDs provided
        '''
        
        
        #-------------------------------------------
        # [2-4] Notify Slack when the DAG End 
        #------------------------------------------ 
        notify_end = SlackWebhookOperator(
                task_id="Notify_End",
                slack_webhook_conn_id="slack_webhook",
                message="""
:tada: DAG finished successfully !
*DAG Name: `{{ dag.dag_id }}`*
*Run ID: `{{ run_id }}`* 
                    """)
        
        #-------------------------------------------
        # [2-5] Notify Slack when the DAG End with failure 
        #------------------------------------------ 
        notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id="slack_webhook",
        message="""
:rotating_light: Warning! 
*DAG: `{{ dag.dag_id }}` failed!*
*Run ID: `{{ run_id }}`*    
*Failed task: `{{ task.task_id }}`*
                    """,
        trigger_rule="one_failed",  # Trigger this task only if the previous task fails
    )

        
        
        Analyse_Records_ >> [feed_analytics_statistics_to_es,feed_analytics_statistics_to_google_sheet] >> notify_end >> notify_failure
        
    # Define task dependencies
    extract_prepare_data >> analyse_data_feeding_GS_ES
