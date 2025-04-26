import scrapy
from scrapy.crawler import CrawlerProcess
import os
import random 
import re
from datetime import datetime
import scrapy 
import pandas as pd
import json
from bs4 import BeautifulSoup 
import requests
from lxml import etree
#temp

class Tools : 
    
    def get_agent_info(source_text):
        'This function extracts agent info from source text'
        item = {}
        agent_address = re.findall(r'"displayAddress":"(.*?)","countryCode"', source_text)[0]
        agent_address_after_clean = agent_address.replace('\\r\\n','')
        item['agent_address']=agent_address_after_clean
        
        agent_info = re.findall(r'"descriptionHTML":"(.*?)"},"', source_text)[0]
        agent_info_after_clean = re.sub('<[^<]+?>', '', agent_info)
        item['agent_info']=agent_info_after_clean
        
        publishedon_text = re.findall(r'{"added":"(.*?)"', source_text)[0]
        item['publishedon'] = publishedon_text[:4]+'-'+publishedon_text[4:6]+'-'+publishedon_text[6:8]
        
        return item
    
    
    def get_description(source_text):
        raw_text = re.findall(r'"description":"(.*?)","propertyPhrase"', source_text)[0]
        clean_text = re.sub(r"<.*?>", "", raw_text)
        return clean_text
        
    
    def current_date():
        current_date = datetime.now()
        formatted_date = current_date.strftime("%d.%m.%Y")
        return formatted_date
    
    
    def get_floorplans_links(source_text):
        floor_links_raw = re.findall(r'"floorplans":(.*?),"virtualTours":', source_text)[0].replace('"','').replace('}','')
        floor_links_list = re.findall(r'url:(.*?),caption:', floor_links_raw)
        floor_links_text = ' , '.join(floor_links_list)
        return floor_links_text
    
    def get_price_history(url):
        'Target of this function is to get the History price of property'
        # define headers for the request
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.120 Safari/537.36'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser").text
        
        try : json_data = json.loads(soup)['soldPropertyTransactions']
        except : json_data = ''
        
        return json_data
    
    def extract_data_source(url):
        'Target of this function is to get the History price'
        # define headers for the request
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.120 Safari/537.36'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        dom = etree.HTML(str(soup))
        raw_data = dom.xpath('//script[contains(text(),"window.PAGE_MODEL = ")]')[0].text
        return raw_data

    def extract_public_transportation(source_text):
        stations_pattern = r'nearestStations(.*?)showSchoolInfo'
        stations_datapoint = re.findall(stations_pattern, source_text)[0]
        stations_datapoint_after_clean = stations_datapoint.replace('":[','').replace('],"','')
        return stations_datapoint_after_clean
    
    def get_media_info(source_text):
        'this function extracts media (video_tours - virtual_Tours) from source text'
        vertual_Tours_pattern = r'virtualTours":(.*?),"customer"'
        media_raw_data = re.findall(vertual_Tours_pattern,source_text)[0]
        links_pattern = r'"(http.*?)"'
        media_links_list = re.findall(links_pattern,media_raw_data)
        media_links_string = ', '.join(media_links_list)
        return media_links_string
    
    def get_pictures(source_text):
        'this function extracts pictures from source text'
        item = {}
        images_patern = r'"url":"(https:\/\/media\.rightmove\.co\.uk\/[a-zA-Z-0-9\/_.jpeg]*)'
        images_list = list(set(re.findall(images_patern,source_text)))
        item['images'] =  ' , '.join(images_list)
        item['numberOfImages'] = len(images_list)
        return item
    
class CollectItemsPipeline:
    def process_item(self, item, spider):
        collected_items.append(item)  # Append item to the global list
        return item  


# Define the Spider
class rightmoveSpider(scrapy.Spider):
    name = "rightmove"    
    user_agents_list = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0 Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64; rv:129.0) Gecko/20100101 Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) CriOS/129.0.0.0 Mobile/15E148 Safari/537.36",
            "Mozilla/5.0 (iPad; CPU OS 14_0 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) CriOS/129.0.0.0 Mobile/15E148 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:129.0) Gecko/20100101 Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Linux; Android 11; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 9; Nexus 6P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 8.1; Nexus 5X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Edg/109.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:129.0) Gecko/20100101 Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; Trident/7.0; rv:11.0) like Gecko Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Linux; U; Android 8.0; en-us; Nexus 9 Build/OPR6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; AS; rv:129.0) like Gecko Chrome/129.0.0.0 Safari/537.36"]

    def start_requests(self):
        # chanels_list  = ['BUY', 'NEW_HOME' , 'RENT' , 'COMMERCIAL_BUY' , 'COMMERCIAL_RENT'] # COMMERCIAL_BUY # COMMERCIAL_RENT
        
        chanels_dict = {1:'BUY' , 2:'NEW_HOME' , 3:'RENT',4:'COMMERCIAL_BUY',5:'COMMERCIAL_RENT'}
        for element in chanels_dict:print(f'please press {element} and enter for [ {chanels_dict[element]} ]')
        channel_index = int(input('Enter a channel Number : '))
        
        
        location_identifier = [f'REGION%5E{i}' for i in range(1,27762)]  # list for all locations - original = 27762  #temp
        for location_id in location_identifier: #temp
            for index_page in range(1050):  # 1050  #temp
                print(f'chanel {chanels_dict[channel_index]} ,Page : {index_page},location identifier : {location_id}')
                url_endpoint = f'https://www.rightmove.co.uk/api/_search?locationIdentifier={location_id}&numberOfPropertiesPerPage=100&radius=10.0&index={index_page}&includeSSTC=false&viewType=LIST&channel={chanels_dict[channel_index]}&maxDaysSinceAdded=7&areaSizeUnit=sqft&currencyCode=GBP&isFetching=false'  # flag for recent records for the last day = &maxDaysSinceAdded=1
                print(url_endpoint)
                yield scrapy.Request(url=url_endpoint , callback=self.parse_api ,
                                    headers= {'USER_AGENT': random.choice(rightmoveSpider.user_agents_list)})
            
            
    def parse_api(self,response):
        if response.status == 200:
            item = {}
            response = response.json()
            print('*'*100)
            elements_list = response['properties']
            # print(f'count of elements {len(elements_list)}')

            for element in elements_list: # count of records in every api page #temp
                item = {} 
                item['extracted_date'] = Tools.current_date()
                item['source'] = 'rightmove'
                item['propertyUrl'] = 'https://www.rightmove.co.uk'+element['propertyUrl']
                item['id'] = element['id']
                item['beds'] = element['bedrooms']
                item['bathrooms'] = element['bathrooms']
                item['Address'] = element['displayAddress']
                item['Sqft'] = element['displaySize']
                item['Type'] = element['channel']
                item['latitude'] = element['location']['latitude']
                item['longitude'] = element['location']['longitude']
                item['price'] = element['price']['displayPrices'][0]['displayPrice'].replace('\u00a3','')
                item['contactTelephone'] = element['customer']['contactTelephone']
                item['agent_name'] = element['customer']['branchDisplayName']
                item['status'] = 'Available'
                item['agent_address'] = element['formattedBranchName']
                item['agent_info'] = element['formattedBranchName']

                header_request = {
                    'Sec-Ch-Ua': '"Not;A=Brand";v="24", "Chromium";v="128"',
                    'Sec-Ch-Ua-Platform': '"Windows"',
                    'Traceparent': '00-70947403a53acdfe01f979f9b3ed2fa2-18bc8a636faa8154-00',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Sec-Ch-Ua-Mobile': '?0',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.120 Safari/537.36',
                    'Content-Type': 'application/json',
                    'Accept': '*/*',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Dest': 'empty',
                    'Referer': 'https://www.rightmove.co.uk/properties/153239183',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Priority': 'u=4, i',
                    'Connection': 'keep-alive'
                }
                            
                yield scrapy.Request(url = item['propertyUrl'],
                                    headers=header_request, 
                                    callback=self.parse_every_page,
                                    meta = {'item':item})
                
    def parse_every_page(self,response):
        item = response.meta.get('item')
        print(f'Target Link is : {item["propertyUrl"]}')
        try : item['summary'] = ' , '.join(response.xpath('//h2[contains(text(),"Key features")]//following-sibling::ul//li/text()').getall())
        except : item['summary'] = ''
        
        # Extract additional details from source text
        source_text = Tools.extract_data_source(item['propertyUrl'])
        item['floorplan'] = Tools.get_floorplans_links(source_text)
        item['public_transportation'] = Tools.extract_public_transportation(source_text)
        item['description'] = Tools.get_description(source_text)
        item['Title'] = re.findall(r'"pageTitle":"(.*?)","shortDescription"', source_text)[0].replace('"','').replace('}','')
        item['virtual_Tours'] = Tools.get_media_info(source_text)
        item['images'] = Tools.get_pictures(source_text)['images']
        item['numberOfImages'] = Tools.get_pictures(source_text)['numberOfImages']
        item['pricehistory'] = Tools.get_price_history(item['propertyUrl'])
        item['public_transportation'] = Tools.extract_public_transportation(source_text)
        item['agent_address'] = Tools.get_agent_info(source_text)['agent_address']
        item['agent_info'] = Tools.get_agent_info(source_text)['agent_info']
        item['publishedon'] = Tools.get_agent_info(source_text)['publishedon']
        print(item)
        
        yield item
        
def run_spider(spider_class):
    global collected_items
    collected_items = []
    process = CrawlerProcess(settings=custom_settings)
    process.crawl(spider_class)
    process.start()
    return collected_items

# Define the settings directly within the file
folder_path = os.path.join(os.getcwd(), "output")
custom_settings = {
    'ROBOTSTXT_OBEY': False,  # Respect robots.txt rules
    'LOG_LEVEL': 'ERROR',  # Reduce logging output for cleaner console output
    'CONCURRENT_REQUESTS' : 16,
    'DOWNLOAD_DELAY' : 0.5,
    'ITEM_PIPELINES': { '__main__.CollectItemsPipeline': 1 }}



# Main block to run the spider
def Main():
    run_spider(rightmoveSpider)
    # script_directory = os.path.dirname(os.path.abspath(__file__))
    target_path = r"/root/airflow/include/onthemarket_resources/output/rightmove_output.csv"
    df = pd.DataFrame(collected_items)
    df.to_csv(target_path,index = False)


#temp Temp
