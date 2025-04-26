from .ElasticSearch import Handling_ElasticSearch

class update_status:
    '''
    This class for update the status of records in ElasticSearch according to the list of IDs provided.
    '''
    
    def __init__(self, list_id_delete_records):
        self.list_id_delete_records = list_id_delete_records
        
    def Main(self):
        Handling_ElasticSearch().update_records_status(self.list_id_delete_records)
