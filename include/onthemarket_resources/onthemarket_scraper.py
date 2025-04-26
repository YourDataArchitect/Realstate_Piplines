import scrapy
from scrapy.crawler import CrawlerProcess
import os
import re
from datetime import datetime
import random 
from bs4 import BeautifulSoup # type: ignore
import requests
from lxml import etree  # Import etree for XPath support
import pandas as pd
import json






# Define the Item (data structure)
class Tools:
    
    def get_locations():
        current_path = os.path.dirname(os.path.abspath(__file__))
        target_file_path = os.path.join(current_path,'postcodes.csv')
        df = pd.read_csv(target_file_path)
        list_postcodes = df['Postcode'].to_list()
        list_postcodes_after_edit = [record.replace(' ','-').lower() for record in list_postcodes]
        return list_postcodes_after_edit
    
    
    def get_meta_data(url):
        'Target of this function is to get the latitude and longitude of a given URL'
        # define headers for the request
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        dom = etree.HTML(str(soup))
        raw_data = dom.xpath('//script[@id="__NEXT_DATA__"]')[0].text
        meta_json = json.loads(raw_data)
        
        item = {}
        
        # extract coordinates
        google = meta_json['props']['pageProps']['property']['staticMapUrls']
        google_link_text = google[list(google.keys())[0]]
        google_api_re = r'https:\/\/maps\.googleapis\.com\/maps\/api\/staticmap\?center=([0-9\.-]+)%2C([0-9\.-]+)'
        Coordinates = re.findall(pattern =google_api_re , string = google_link_text)[0]
        item['latitude'] = Coordinates[0]
        item['longitude'] = Coordinates[1]

        # Extract public transportation stations
        stations_list = meta_json['props']['pageProps']['property']['station']
        public_transportation = ' , '.join([element['name']+ '( ' +element['displayDistance']+' )'  for element in stations_list])
        item['public_transportation'] = public_transportation

        # Extract agent information
        item['agent_info'] = meta_json['props']['pageProps']['property']['agent']['description']

        # Extract images 
        images_list = [element['largeUrl'] for element in meta_json['props']['pageProps']['property']['images']]
        item['images'] = ' , '.join(images_list)
        item['count_images'] = len(images_list)
        item['floorplan'] = ' , '.join([element['largeUrl'] for element in meta_json['props']['pageProps']['property']['floorplans']])
        item['summary'] = ' , '.join([element['feature'] for element in meta_json['props']['pageProps']['property']['features']])
        
            
        
        return item

    def current_date():
        current_date = datetime.now()
        formatted_date = current_date.strftime("%d.%m.%Y")
        return formatted_date
        



class CollectItemsPipeline:
    def process_item(self, item, spider):
        collected_items.append(item)  # Append item to the global list
        return item  


# Define the Spider
class onthemarketSpider(scrapy.Spider):
    name = "onthemarket"    
    
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

    
    
    # onthemarket_1 >>Tag_list[:1] ok 
    # onthemarket_2 >>Tag_list[1:] ok

    


        
    def start_requests(self):
        list_locations = Tools.get_locations()
        # print(f'******** count of all links :: {len(list_locations)}')
        Tag_list = ['/for-sale/property','/to-rent/flats-apartments']
        for tag in Tag_list: 
            for location in list_locations : 

                #https://www.onthemarket.com/for-sale/property/dn33/
                main_link_after_edit = f'https://www.onthemarket.com{tag}/{location}?radius=10.0&recently-added=7-days'  #you can edit the link for every area for the last 24 hours - &recently-added=24-hours
                print(f"[{datetime.now()}] Crawling URL: {main_link_after_edit}")
                yield scrapy.Request(url=main_link_after_edit , callback=self.parse_level_1 ,
                                    headers= {'USER_AGENT': random.choice(onthemarketSpider.user_agents_list)})
                
    def parse_level_1(self, response):  
        list_urls = response.xpath('//div[@class="otm-PropertyCardMedia"]/div[1]/a[contains(@href,"details")]/@href').getall()
       
        
        
        for link in response.xpath('//div[@class="otm-PropertyCardMedia"]'): 
            page_code = link.xpath('./div[1]/a[contains(@href,"details")]/@href').get()
            if page_code != None : 
                # print(f'******** page_code :: {page_code}')
                link = f'https://www.onthemarket.com{page_code}'
                # print(f'Page link : {link}')
                print(f"[{datetime.now()}] Crawling URL: {link}")
                yield scrapy.Request(url = link , callback=self.parse_level_2 , meta = {'url': link} ,
                            headers= {'USER_AGENT': random.choice(onthemarketSpider.user_agents_list)})
                
        next_buttom = response.xpath('//a[@title="Next page"]/@href').get()
        if next_buttom:
            print(f'Next page :::: {"https://www.onthemarket.com"+next_buttom}')
            next_buttom_after_edit = 'https://www.onthemarket.com'+next_buttom
            yield scrapy.Request(url = next_buttom_after_edit , callback=self.parse_level_1 , 
                                headers= {'USER_AGENT': random.choice(onthemarketSpider.user_agents_list)})
        
    def parse_level_2(self,response):
        url = response.meta.get('url')
        # print(f'Target url : {url}')
        # print(f'Request Status : {response.status}')
        print(f"[{datetime.now()}] Request Status: {response.status}")
        
        item = {}
        item['propertyUrl'] = url
        item['extracted_date'] = Tools.current_date()
        item['source'] = 'onthemarket'
        item['id'] = item['propertyUrl'].replace('https://www.onthemarket.com/details/','').replace('/','').replace('?recently-added=24-hours','')
        item['price'] = response.xpath('//*[@data-test="property-price"]/text()').get().replace('\u00a3','')
        item['Title'] = response.xpath('//h1/text()').get()
        item['Address'] = response.xpath('//div[@class="text-slate text-body2 font-normal leading-none font-heading md:leading-normal"]/text()').get()
        
        try : item['beds'] = response.xpath('//div[@class="leading-none whitespace-nowrap" and contains(text(),"bed")]/text()').get().split(' ')[0]
        except : item['beds'] = 'None'
        
        if 'pcm' in item['price'] : item['Type'] = 'For Rent'
        else : item['Type'] = 'For Sale'
        
        item['status'] = 'Available'
        item['Sqft'] = response.xpath('//div[contains(text()," sq m")]//text()|//div[contains(text(),"acre")]//text()').get()
        
        description_list = response.xpath('//div[@item-prop="description"]//text()|//div[@itemprop="description"]//text()').getall()
        item['description'] = ' ,'.join(description_list)
        
        item['agent_name'] = response.xpath('//div[@class="text-sm font-bold mt-2.5"]/text()').get()
        item['agent_address'] = response.xpath('//div[@class="text-xs text-slate"]/text()').get()
        item['contactTelephone'] = response.xpath('//div[@class="_3L5_5G text-link font-semibold flex items-center lg:text-1xl xl:text-2xl mb-3"]/text()').get()
        item['virtual_Tours'] = response.xpath('//h2[contains(text(),"Video tours")]/following-sibling::ul//a/@href').get()
        item['bathrooms'] = response.xpath('//div[@class="leading-none whitespace-nowrap" and contains(text(),"bath")]/text()').get()


        #  Extract Data points from meta data
        meta_date = Tools.get_meta_data(item['propertyUrl'])
        item['latitude']  = meta_date['latitude']
        item['longitude']  = meta_date['longitude']
        item['public_transportation'] = meta_date['public_transportation']
        item['agent_info'] = meta_date['agent_info']
        item['images'] = meta_date['images']
        item['numberOfImages'] = meta_date['count_images']
        item['floorplan'] = meta_date['floorplan']
        item['summary'] = meta_date['summary']
        item['pricehistory'] = ''
        
        
        
        
 
        # print(item)
        yield item
        
        


# Define the settings directly within the file
custom_settings = {
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',  # Set a user agent to avoid blocking
    'ROBOTSTXT_OBEY': False,  # Respect robots.txt rules
    'LOG_LEVEL': 'ERROR',  # Reduce logging output for cleaner console output
    'CONCURRENT_REQUESTS' : 16,  #3
    'DOWNLOAD_DELAY' : 0.1, # 1
    'ITEM_PIPELINES': { '__main__.CollectItemsPipeline': 1 }}



def run_spider(spider_class):
    global collected_items
    collected_items = []
    process = CrawlerProcess(settings=custom_settings)
    process.crawl(spider_class)
    process.start()
    return collected_items


# Main block to run the spider
def Main(): 
# if __name__ == "__main__":
    run_spider(onthemarketSpider)
    # script_directory = os.path.dirname(os.path.abspath(__file__))
    # target_path = r"/root/airflow/include/onthemarket_resources/output/onthemarket_output.csv"
    target_path = r"/usr/local/airflow/include/onthemarket_resources/output/onthemarket_output.csv"

    df = pd.DataFrame(collected_items)
    df.to_csv(target_path,index=False)


 