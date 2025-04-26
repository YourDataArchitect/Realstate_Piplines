import re
import os 
import pandas as pd 
import ijson
from datetime import datetime





def get_last_recent_JSON_file():
    '''
    This function for compare json file to get the last recent file
    Return : Path for the last recent JSON file
    '''
    
    # Get all json files 
    current_dir = os.path.dirname(os.path.abspath(__file__))
    download_page_folder_path = os.path.join(current_dir, "output","zoopla_listing_data.json")
    return download_page_folder_path

def parse_batch(list_dicts):
    'input is records as list of dictionaries'
    list_records = []

    for i in range(len(list_dicts)):
        for data_dict in list_dicts[i]['all_information']['listings']['regular']:
            item = {}
            item['title'] = data_dict['title']
            item['listing_id'] = data_dict['listingId']
            
            try : 
                description = data_dict['detail_page']['data']['listingDetails']['detailedDescription']
                clean_description = re.sub(r'<[^>]+>', '', description)
                item['description '] = clean_description
            except : 
                item['description '] = None
                
            # print(data_dict)
            item['url'] = 'https://www.zoopla.co.uk'+data_dict['detail_page']['data']['listingDetails']['listingUris']['detail']
            item['category'] = data_dict['detail_page']['data']['listingDetails']['category']
            item['section'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['section']
            item['address'] = data_dict['detail_page']['data']['listingDetails']['displayAddress']
            item['postcode'] = data_dict['detail_page']['data']['listingDetails']['location']['postalCode']
            item['latitude'] = data_dict['detail_page']['data']['listingDetails']['location']['coordinates']['latitude']
            item['longitude'] = data_dict['detail_page']['data']['listingDetails']['location']['coordinates']['longitude']
            item['street_name'] = data_dict['detail_page']['data']['listingDetails']['location']['streetName']
            item['town_city'] = data_dict['detail_page']['data']['listingDetails']['location']['townOrCity']
            item['country'] = data_dict['detail_page']['data']['listingDetails']['location']['countryCode']
            item['property_type'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['propertyType']
            item['bedrooms'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['numBeds']
            item['bathrooms'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['numBaths']
            item['tenure'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['tenure']
            item['reception_rooms'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['numRecepts']
            item['price'] = data_dict['detail_page']['data']['listingDetails']['pricing']['valueLabel']
            item['number_of_images'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['numImages']
            item['price_qualifier'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['priceQualifier']
            item['status'] = data_dict['detail_page']['data']['listingDetails']['publicationStatus']
            item['uprn'] = data_dict['detail_page']['data']['listingDetails']['location']['uprn']
            item['property_number_or_name'] = data_dict['detail_page']['data']['listingDetails']['location']['propertyNumberOrName']
            item['floor_area'] = data_dict['detail_page']['data']['listingDetails']['pricing']['pricePerFloorAreaUnit']
            item['floor_area_units'] = data_dict['detail_page']['data']['listingDetails']['floorArea']
            item['epc_rating'] = data_dict['detail_page']['data']['listingDetails']['derivedEPC']
            item['furnished_state'] = data_dict['detail_page']['data']['listingDetails']['features']['flags']['furnishedState']
            item['is_retirement_home'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['isRetirementHome']
            item['is_shared_ownership'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['isSharedOwnership']
            item['images'] =  data_dict['imageUris']
            item['image_preview'] =  data_dict['detail_page']['data']['listingDetails']['imagePreview']
            item['floor_plans'] =  data_dict['detail_page']['data']['listingDetails']['floorPlan']
            item['virtual_tours'] = data_dict['detail_page']['data']['listingDetails']['content']['virtualTour']
            item['audio_tours'] = data_dict['detail_page']['data']['listingDetails']['content']['audioTour']
            item['highlights'] = data_dict['detail_page']['data']['listingDetails']['features']['highlights']
            item['tags'] = data_dict['tags']
            item['bullets'] = data_dict['detail_page']['data']['listingDetails']['features']['bullets']
            item['flags'] = data_dict['detail_page']['data']['listingDetails']['features']['flags']
            item['price_per_sqft'] = data_dict['detail_page']['data']['listingDetails']['pricing']['pricePerFloorAreaUnit']
            item['is_auction'] = data_dict['detail_page']['data']['listingDetails']['pricing']['isAuction']
            item['sync_status'] = 'Active'
            #--------------------------- please check the last sale data
            
            print(item['url'])
            try : item['price_changes'] = data_dict['detail_page']['data']['listingDetails']['priceHistory']['priceChanges']
            except : item['price_changes'] = None

            try : item['first_published_date'] = data_dict['detail_page']['data']['listingDetails']['priceHistory']['firstPublished']['firstPublishedDate']
            except : item['first_published_date'] = None

            try : item['first_published_date'] = data_dict['detail_page']['data']['listingDetails']['priceHistory']['firstPublished']['firstPublishedDate']
            except : item['first_published_date'] = None

            try : item['first_published_price'] = data_dict['detail_page']['data']['listingDetails']['priceHistory']['firstPublished']['priceLabel']
            except : item['first_published_price'] = None

            try : item['last_sale_date'] = data_dict['detail_page']['data']['listingDetails']['priceHistory']['firstPublished']['firstPublishedDate']
            except : item['last_sale_date'] = None
            
            try : item['last_sale_price']  = data_dict['detail_page']['data']['listingDetails']['priceHistory']['firstPublished']['priceLabel']
            except : item['last_sale_price']  = None
            
            
            try : last_sale = data_dict['detail_page']['data']['listingDetails']['priceHistory'].get('lastSale')
            except : last_sale = None
            item['last_sale_price_label'] = last_sale.get('priceLabel') if last_sale else None   # newBuild
            item['last_sale_new_build'] = last_sale.get('newBuild') if last_sale else None 
            item['last_sale_recently_sold'] = last_sale.get('recentlySold') if last_sale else None 
            
            
            try : priceChanges = data_dict['detail_page']['data']['listingDetails']['priceHistory']['priceChanges']
            except : priceChanges = None
            if priceChanges != None : item['price_changes'] = priceChanges
            else : item['price_changes'] = None
        
            item['price_change_price'] = last_sale.get('price') if last_sale else None 
            item['price_change_price_label'] = last_sale.get('priceLabel') if last_sale else None 
            item['agent_name'] = data_dict['detail_page']['data']['listingDetails']['branch']['name']
            item['agent_phone'] = data_dict['detail_page']['data']['listingDetails']['branch']['phone']
            item['agent_address'] = data_dict['detail_page']['data']['listingDetails']['branch']['address']
            item['agent_postcode'] = data_dict['detail_page']['data']['listingDetails']['branch']['postcode']
            item['agent_logo'] = data_dict['detail_page']['data']['listingDetails']['branch']['logoUrl']
            
            agent_profile = data_dict['detail_page']['data']['listingDetails']['branch']['profile']['details']
            if agent_profile != None : 
                clean_agent_profile = re.sub(r'<[^>]+>', '', agent_profile)
                item['agent_profile'] = clean_agent_profile
            else : item['agent_profile'] = agent_profile
            
            
            
            item['agent_member_type'] = data_dict['detail_page']['data']['listingDetails']['branch']['memberType']
            item['points_of_interest'] = data_dict['detail_page']['data']['listingDetails']['pointsOfInterest']
            item['area_name'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['areaName']
            item['listings_category'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['listingsCategory']
            item['county_area_name'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['countyAreaName']
            item['listing_condition'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['listingCondition']
            item['post_town_name'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['postTownName']
            item['region_name'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['regionName']
            item['location'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['location']
            item['size_sq_feet'] = data_dict['detail_page']['data']['listingDetails']['analyticsTaxonomy']['sizeSqFeet']
            
            list_records.append(item)
    return list_records
            # list_data_after_parse.append(item)
            # print(item)



def read_json_in_batches(file_path, batch_size=200, maximum_records = 1000000000000000 ):
    batch = []
    with open(file_path, 'r', encoding='utf-8') as f:
        records_max_counter = 0 
        for item in ijson.items(f, 'item'):
            records_max_counter+=1 
            batch.append(item)
            if records_max_counter < maximum_records:
                if len(batch) == batch_size :
                    print(f'Count Data Processing : {len(batch)}')
                    yield batch
                    batch = []
            else : 
                print(f'Count Data Processing : {len(batch)}')
                yield batch
                break

def saving_records(list_records) :
    df = pd.DataFrame(list_records)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    time_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f'district_listing_data_{time_stamp}.csv'
    download_page_folder_path = os.path.join(current_dir, "output" ,file_name )
    df.to_csv(download_page_folder_path,encoding='utf-8-sig', index=False)
    


def Main():
    
    # Reading json file as batches
    last_recent_json_path = "/root/airflow/include/zoopla_resources/output/zoopla_listing_data.json"
    # last_recent_json_path = r"D:\projects\Solomon\Scrapers\zoopla\output\district_listing_data_20250324_225106.json"
    list_records_gross = []
    for batch in read_json_in_batches(last_recent_json_path, batch_size=500):
        print(f"Processing batch of {len(batch)} items")
        list_records = list(parse_batch(batch))
        
        for record in list_records:
            list_records_gross.append(record)
        
        
    # saving data_after parsing to csv file 
    saving_records(list_records_gross)
        
    
        

