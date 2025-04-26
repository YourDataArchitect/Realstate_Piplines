from .ElasticSearch import Handling_ElasticSearch
import os
import pandas as pd
from datetime import datetime



class AddNewRecords:
    '''
    This class for add new records to ElasticSearch.
    '''
    
    def __init__(self, List_id_new_records):
        self.List_id_new_records = List_id_new_records
        
    
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
            item['latest_recent_path'] = latest_recent_path

            return item
            
        except Exception as e :
            print(f'There are Error : {e} in comparing csv files names')
            pass  

    

        
    def Main(self):
        
        path_last_csv_file = AddNewRecords().get_last_recent_csv_file()
        
        # Get list of listing_id from the last recent version
        try : 
            df = pd.read_csv(path_last_csv_file)
            filtered_df = df[df['listing_id'].isin(self.List_id_new_records)]
            
            list_records_filter = filtered_df.to_dict(orient='records')
            for record in list_records_filter:
                record['sync_status'] = 'Active'
                record['sync_date'] = datetime.now().strftime('%Y-%m-%d')
                
                Handling_ElasticSearch().add_document(record)
        except Exception as e:
            print(f'There are Error : {e} in adding new records to ElasticSearch')
            pass 
            
        Handling_ElasticSearch().add_bulk(self.index_name, self.file_path)