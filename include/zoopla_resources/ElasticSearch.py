from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import scan
import json
from datetime import datetime
import os
import pandas as pd
import numpy as np


class Tools : 
    
    def chunk_data(data, chunk_size = 500):
        
        for i in range(0, len(data), chunk_size):
            print(f'Chunk data size {chunk_size} of records')
            yield data[i:i + chunk_size]
    
    def clean_dataset(dataset_path):
        #this function for clean datapoints (beds - bathrooms)
        df = pd.read_excel(dataset_path)
        df['beds'] = df['beds'].str.split(' ').str[0]
        df['bathrooms'] = df['bathrooms'].str.split(' ').str[0]
        df.to_excel(dataset_path)

class Handling_ElasticSearch : 
    
    def __init__(self):
        link = "https://95.217.117.251:9200"
        username = 'elastic'
        passwrord = 'd41=*sDuOnhQqXonYz2U'
        
        # Connect to Elasticsearch
        self.es = Elasticsearch(
            link,
            basic_auth=(username, passwrord),  # Remove if authentication is not required
            verify_certs=False  # Set to True if you have a valid SSL certificate
        )
        # Check the connection
        try:
            print(self.es.info())  # Print Elasticsearch cluster info
        except Exception as e:
            print(f"Error connecting to Elasticsearch: {e}")
            
    def add_bulk(self,index_name,file_path):
        '''
        This function adds multiple documents to the specified index using bulk API.
        Param : 
            file_path : The path of the CSV file
        '''
 
        df = pd.read_csv(file_path) 
        
        # **Fix: Convert NaN to None (which becomes null in JSON)**
        df.replace({np.nan: None}, inplace=True)
        
        
        chunk_counter = 0 
        for chunk in Tools.chunk_data(df):
            
            chunk_counter += len(chunk)
            print(f'Total records {chunk_counter}')
            df_chunk = pd.DataFrame(chunk)
                        
            # Delete unwanted columns result from convert json to CSV
            if "Unnamed: 0" in df_chunk.columns:df_chunk = df_chunk.drop(columns=["Unnamed: 0"])
            if "Unnamed: 0.1" in df_chunk.columns:df_chunk = df_chunk.drop(columns=["Unnamed: 0.1"])
            
            # Convert DataFrame to Elasticsearch bulk format
            bulk_data = [{"_index": index_name, "_source": row.to_dict()} for _, row in df_chunk.iterrows()]
            
            try:
                success, errors = helpers.bulk(self.es,
                                               bulk_data,
                                               raise_on_error=False,
                                               request_timeout=300) # Give ES more time to consume each bulk
                print(f"Successfully inserted {success} documents.")
                if errors:
                    print("Errors occurred while inserting data:")
                    for error in errors:
                        print(error)  # Print each error to identify the issue
            except helpers.BulkIndexError as e:
                print("Bulk index error occurred:", e.errors)     
                
    def add_document(self,
                      index_name:str,
                      document:json):
        '''
        This function adds single document to the specified index.
        Param : 
            index_name : The name of the index
            document : one record to add to the index (format is JSON)
        '''
        
        self.es.index(
            index = index_name,
            document= document
        )

    def display_index(self, index_name):
        'This function displays all records in the specified index'
        
        response = self.es.search(index=index_name, body={"query": {"match_all": {}}})
        
        # display count of documents
        print(f'Count of documents is : {len(response["hits"]["hits"])}')
        
        # Extract and display results
        for hit in response["hits"]["hits"]:
            print(hit["_source"])  # Displays the document data
        
    def update_records_status(self,list_records_id):
        for record_id in list_records_id:
            # Prepare the query
            body ={
                    "script": {
                        "source": """
                            ctx._source.sync_status = params.state;
                            ctx._source.sync_date = params.date;
                        """,
                        "lang": "painless",
                        "params": {
                            "state": "Deleted",
                            "date": datetime.now().strftime('%Y-%m-%d')
                        }
                    },
                    "query": {
                        "term": {
                            "listing_id": record_id
                        }
                    }
                }

            try : 
                # Execute update_by_query
                self.es.update_by_query(index="zoopla_sync", body=body)

                # Print the response (optional)
                print(f"The record ID: {record_id} status was updated to [Deleted] because it does not exist in the most recent version of the data.")
                
            except Exception as error : 
                print(f'There are Error in update record ID : {record_id} , Error : {error}')

        
        
        
    def clean_index(self,index_name):
        'This function deletes all records in the specified index'
        
        self.es.indices.delete(index=index_name)
        print(f'Index {index_name} has been deleted')
    
    def Extract_id_records(self):
        # Use scan to retrieve all documents (efficient for large data)
        listing_ids = []

        # Scan through all documents in the index
        for doc in scan(self.es, index="zoopla_sync", query={"_source": ["listing_id"], "query": {"match_all": {}}}):
            listing_id = doc['_source'].get('listing_id')
            print(f'count records is : {len(listing_ids)}')
            if listing_id is not None:
                listing_ids.append(listing_id)

        # Print total count or use the list
        
        print(f"Total listing IDs found: {len(listing_ids)}")
        return listing_ids
        # print(listing_ids)  # Optional, if you want to view them
        
        