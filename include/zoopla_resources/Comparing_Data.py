
import sys
import os
import pandas as pd 
import os
from datetime import datetime
from .ElasticSearch import Handling_ElasticSearch



def get_last_recent_csv_file():
    '''
    This function for compare csv file to get the last recent file
    Return : Path for the last recent JSON file
    '''
    item = {}
    
    # Get all csv files 
    current_dir = os.path.dirname(os.path.abspath(__file__))
    download_page_folder_path = os.path.join(current_dir, "output")
    json_list = [file for file in os.listdir(download_page_folder_path) if file.endswith('csv')]
    
    # compare_csv_files
    try: 
        parse_date = lambda element : datetime.strptime(element,'district_listing_data_%Y%m%d_%H%M%S.csv')
        last_titles_list = [parse_date(item) for item in json_list if 'district_listing_data' in item and '_After_update' not in item]
        last_titles_list_after_sort = sorted(last_titles_list,reverse=True)
        
        # Define the last file 
        latest_recent_file = f'district_listing_data_{last_titles_list_after_sort[0].strftime("%Y%m%d_%H%M%S")}.csv'
        latest_recent_path = os.path.join(current_dir, "output",latest_recent_file)
        return latest_recent_path
        
    except Exception as e :
        print(f'There are Error : {e} for GET last recent CSV file path')
        pass  


def comparing_records(list_id_csv , list_id_elasticsearch , flag ):
    '''
    func for get the new records and delete records from the last recent version
    flag = 'new_records' or 'delete_records'
    '''
    
    if flag == 'new_records' : 
        # Get ID's of records not found in the last recent version 
        id_new_records = list(set(list_id_csv) - set(list_id_elasticsearch))
        return id_new_records
    
    elif flag == 'delete_records' :
        # Get ID's of records not found in the last recent version 
        id_delete_records = list(set(list_id_elasticsearch) - set(list_id_csv))
        return id_delete_records





def Main(**kwargs): # we use **kwargs to get the context of the task instance for airflow
    
    path_last_csv_file = get_last_recent_csv_file()
    print(f'Path for the last file : {path_last_csv_file}')
    
    list_id_elasticsearch = Handling_ElasticSearch().Extract_id_records()
    
    
    # Get list of listing_id from the last recent version
    list_id_last_file = pd.read_csv(path_last_csv_file,
                                    usecols=['listing_id'],
                                    low_memory=True ,
                                    dtype={'listing_id': int})['listing_id'].tolist()
    
    # Comparing data from the last version vs previous version
    
    # [1] Get the Deleted Records from the last recent version
    list_new_records = comparing_records(list_id_csv =  list_id_last_file ,
                                                list_id_elasticsearch  = list_id_elasticsearch,
                                                flag = 'new_records')

    list_delete_records = comparing_records(list_id_csv =  list_id_last_file ,
                                                list_id_elasticsearch  = list_id_elasticsearch,
                                                flag = 'delete_records')


    print(f'After comparing data sets found count {len(list_delete_records)} are deleted records , And {len(list_new_records)} are new records')
    
    
    # Push both lists to XComs
    ti = kwargs['ti']
    ti.xcom_push(key='new_records', value=list_new_records)
    ti.xcom_push(key='delete_records', value=list_delete_records)

    return 'Comparison done'
    
    
    # item = {}
    # item['list_new_records'] = list_new_records
    # item['list_delete_records'] = list_delete_records
    
     


    
    
    
    
    # # Update status for records 
    # list_records_after_update = update_status(path_previous_file,list_id_delete_records)
    
    # # Save records after sync to csv file 
    # df = pd.DataFrame(list_records_after_update)
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # path_output_dataset_after_update_status = os.path.join(current_dir, "output",'Dataset_after_update_status.csv')
    # df.to_csv(path_output_dataset_after_update_status)
            

    
    