from onthemarket_resources import onthemarket_scraper
from onthemarket_resources import ElasticSearch
from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import logging




    

def get_path_csv()-> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    scraped_data_path = os.path.join(current_dir, "output","onthemarket_output.csv")
    return scraped_data_path

def feed_elasticsearch(**_):   # so we use Wrapper for not calling the function directly
    """Wrapper called by PythonOperator."""
    index = "data_scraping"
    logging.warning("===> Feeding Elastic Search is starting")
    csv_path = r"/usr/local/airflow/include/onthemarket_resources/output/onthemarket_output.csv"
    ElasticSearch.Handling_ElasticSearch().add_bulk(index, csv_path)
    logging.warning("===> Feeding Elastic Search is Ending")
    

def run_spider_wrapper(**_):
    """
    Wrapper that starts the Scrapy spider.
    Putting the import INSIDE the function
    keeps DAG-parsing light and lets logs appear.
    """
    logging.warning("===> On The Market spider is starting")
    onthemarket_scraper.Main()
    logging.warning("===> On The Market spider finished")


with DAG('OnTheMarket_PipeLine',
         default_args={
             'owner': 'Home Portfolio',
             'depends_on_past': False,
             'catchup': False,
             'start_date': datetime(2025, 4, 18),
             'retries': 1,
             'retry_delay': timedelta(minutes=5)
         },
         schedule_interval='@weekly',
         catchup=False) as dag:
    
    #-------------------------------------------
    # [1] Running the spider to scrape data from Onthemarket and return the csv file
    #-------------------------------------------
    run_spider = PythonOperator(task_id = 'Running_OnTheMarket_Spider',
                                python_callable = run_spider_wrapper)  # No parentheses! for the function becasue to prevent it from running immediately and wait for the scheduler to run it 
    
    
    
    #-------------------------------------------
    # [2] Sensor to wait for the spider to finish and create the csv file
    #-------------------------------------------
    Wait_Spider_Finish = FileSensor(
        task_id="Wait_Spider_Finish",
        # filepath=r"/root/airflow/include/onthemarket_resources/output/onthemarket_output.csv", 
        filepath = r"/usr/local/airflow/include/onthemarket_resources/output/onthemarket_output.csv",
        poke_interval=30,  # check every 30 seconds
        fs_conn_id = 'fs_default' , # if you are using a different connection id for the file system, you can specify it here
        timeout=2592000,       # wait max 30 days
        mode="poke"        # or use "reschedule" if your system is big
    )
    Wait_Spider_Finish.doc_md = '''
    sensor for checking if the spider has finished running and CSV file is created 
    -->> it will check every 30 seconds for 3 days
    '''

    #-------------------------------------------
    # [3] Feeding ElasticSearch With scraped data
    #-------------------------------------------
    Feeding_ElasticSearch = PythonOperator(task_id = 'Feeding_ElasticSearch',
                                            python_callable = feed_elasticsearch)  
    
    Feeding_ElasticSearch.doc_md = '''
    Feeding ElaSTICSEARCH with the scraped data from the OnTheMarket spider
    '''

    # Define task dependencies
    run_spider >> Wait_Spider_Finish >> Feeding_ElasticSearch 