# Temp 256 - 277

import time
from base64 import b64decode
from curl_cffi import requests as curl_request
import requests
import pandas as pd
import json
import random
from threading import Thread
import urllib3
from tqdm import tqdm
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    filename=r'D:\scrapy_projects\current_projects\scrapy_projects\solomon\airflow\include\zoopla_resources\output\zoopla_district_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Suppress warnings from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
BATCH_SIZE = 100  # Reduced batch size since districts are fewer
SAVE_INTERVAL = 50  # Save data more frequently since each district has more listings
CHECKPOINT_FILE = r'D:\scrapy_projects\current_projects\scrapy_projects\solomon\airflow\include\zoopla_resources\output\district_checkpoint.json'
TEMP_DATA_FILE = r'D:\scrapy_projects\current_projects\scrapy_projects\solomon\airflow\include\zoopla_resources\output\district_data_temp.json'
SKIPPED_DISTRICTS_FILE = r'D:\scrapy_projects\current_projects\scrapy_projects\solomon\airflow\include\zoopla_resources\output\skipped_districts.txt'
OUTPUT_FILE = r"D:\scrapy_projects\current_projects\scrapy_projects\solomon\airflow\include\zoopla_resources\output\zoopla_listing_data.json"


#Notes : count postcodes = 2309

page_data = list()

ua_list = [
    "OkHttp/4.9.2",
    "OkHttp/4.8.1",
    "OkHttp/4.7.2",
    "OkHttp/4.6.0",
    "OkHttp/4.5.0",
    "OkHttp/4.4.1",
    "OkHttp/4.3.1",
    "OkHttp/4.2.1",
    "OkHttp/4.1.0",
    "OkHttp/4.0.1",
    "OkHttp/3.14.1",
    "OkHttp/3.13.1",
    "OkHttp/3.12.12",
    "OkHttp/3.11.0",
    "OkHttp/3.10.0",
    "OkHttp/3.9.1",
    "OkHttp/3.8.1",
    "OkHttp/3.7.0",
    "OkHttp/3.6.0",
    "OkHttp/3.5.0",
    "OkHttp/3.4.2",
    "OkHttp/3.3.0",
    "OkHttp/3.2.0",
    "OkHttp/3.1.0",
    "OkHttp/3.0.1",
    "OkHttp/2.7.5",
    "OkHttp/2.6.0",
    "OkHttp/2.5.0",
    "OkHttp/2.4.0",
    "OkHttp/2.3.0",
]

# WEBSHARE PROXIES
proxies = {
   'http': 'http://cxmzvqtw-rotate:zgp8h079llzg@p.webshare.io:80',
    'https': 'http://cxmzvqtw-rotate:zgp8h079llzg@p.webshare.io:80',
}

def save_checkpoint(processed_districts, all_data):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({
            'processed_districts': processed_districts,
            'timestamp': datetime.now().isoformat()
        }, f)
    
    # Update temp data file
    with open(TEMP_DATA_FILE, 'w') as f:
        json.dump(all_data, f)

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)['processed_districts']
    return []

def load_temp_data():
    if os.path.exists(TEMP_DATA_FILE):
        with open(TEMP_DATA_FILE, 'r') as f:
            return json.load(f)
    return []

def fetch_listing_details(listing_id):
    global page_data
    listing_details_api = 'https://api-graphql-lambda.prod.zoopla.co.uk/graphql/'
    headers = {
        'accept': '*/*',
        'x-api-key': 'public-7pwHLbsIxzGHwV47PMIrHVyeReosfFiFzpiSqo97wM8',
        'cookie': 'zooplasid=73483403cf484419a32a325535aec20a;zooplapsid=6495751daf594668a0b7d6a875abb2af;',
        'origin': 'zoopla-mobile-app',
        'Content-Type': 'application/json',
        'Content-Length': '8436',
        'Host': 'api-graphql-lambda.prod.zoopla.co.uk',
        'Connection': 'Keep-Alive',
        'Accept-Encoding': 'gzip',
        'User-Agent': 'okhttp/4.9.2',
    }
    payload = {
        "operationName": "getListingDetails",
        "variables": {
            "listingId": int(listing_id),
            "include": ["EXPIRED"]
        },
        "query": "query getListingDetails($listingId: Int!, $include: [ListingInclusion]) {\n  listingDetails(id: $listingId, include: $include) {\n    ...LISTING\n    ... on ListingResultError {\n      errorCode\n      message\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment LISTING on ListingData {\n  listingId\n  administrationFees\n  detailedDescription\n  metaTitle\n  metaDescription\n  category\n  listingUris {\n    detail\n    __typename\n  }\n  title\n  publicationStatus\n  counts {\n    numBedrooms\n    numBathrooms\n    numLivingRooms\n    __typename\n  }\n  viewCount {\n    viewCount30day\n    __typename\n  }\n  ntsInfo {\n    title\n    value\n    __typename\n  }\n  derivedEPC {\n    efficiencyRating\n    __typename\n  }\n  ...AGENT_BRANCH\n  ...LISTING_ANALYTICS_TAXONOMY\n  ...LISTING_ADTARGETING\n  ...LISTING_ANALYTICS_ECOMMERCE\n  ...PRICING\n  ...ENERGY_PERFORMANCE_CERTIFICATE\n  ...LISTING_FEATURES\n  ...FLOOR_PLANS\n  ...FLOOR_AREA\n  ...MEDIA\n  ...MAP\n  ...EMBEDDED_CONTENT\n  ...POINTS_OF_INTEREST\n  ...PRICE_HISTORY\n  ...LISTING_SUMMARY\n  __typename\n}\n\nfragment AGENT_BRANCH on ListingData {\n  branch {\n    ...AGENT_BRANCH_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment AGENT_BRANCH_FRAGMENT on AgentBranch {\n  branchId\n  address\n  branchDetailsUri\n  branchResultsUri\n  logoUrl\n  phone\n  name\n  postcode\n  memberType\n  attributes {\n    embeddedContentIsBlacklisted\n    showOverseasListingExactLocation\n    __typename\n  }\n  profile {\n    details\n    __typename\n  }\n  __typename\n}\n\nfragment LISTING_ANALYTICS_TAXONOMY on ListingData {\n  analyticsTaxonomy {\n    ...LISTING_ANALYTICS_TAXONOMY_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment LISTING_ANALYTICS_TAXONOMY_FRAGMENT on ListingAnalyticsTaxonomy {\n  acorn\n  acornType\n  areaName\n  bedsMax\n  bedsMin\n  branchId\n  branchLogoUrl\n  branchName\n  brandName\n  chainFree\n  companyId\n  countryCode\n  countyAreaName\n  currencyCode\n  displayAddress\n  furnishedState\n  groupId\n  hasEpc\n  hasFloorplan\n  incode\n  isRetirementHome\n  isSharedOwnership\n  listingCondition\n  listingId\n  listingsCategory\n  listingStatus\n  location\n  memberType\n  numBaths\n  numBeds\n  numImages\n  numRecepts\n  outcode\n  postalArea\n  postTownName\n  priceActual\n  price\n  priceMax\n  priceMin\n  priceQualifier\n  propertyHighlight\n  propertyType\n  regionName\n  section\n  sizeSqFeet\n  tenure\n  uuid\n  zindex\n  __typename\n}\n\nfragment LISTING_ADTARGETING on ListingData {\n  adTargeting {\n    ...LISTING_ANALYTICS_TAXONOMY_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment LISTING_ANALYTICS_ECOMMERCE on ListingData {\n  analyticsEcommerce {\n    ...LISTING_ANALYTICS_ECOMMERCE_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment LISTING_ANALYTICS_ECOMMERCE_FRAGMENT on ListingAnalyticsEcommerce {\n  brand\n  category\n  id\n  name\n  price\n  quantity\n  variant\n  __typename\n}\n\nfragment PRICING on ListingData {\n  pricing {\n    ...PRICING_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment PRICING_FRAGMENT on ListingPricing {\n  isAuction\n  qualifier\n  priceQualifierLabel\n  internalValue\n  rentFrequencyLabel\n  valueLabel\n  currencyCode\n  originalCurrencyPrice {\n    internalValue\n    rentFrequencyLabel\n    unitsLabel\n    label\n    valueLabel\n    currencyCode\n    __typename\n  }\n  pricePerFloorAreaUnit {\n    internalValue\n    rentFrequencyLabel\n    unitsLabel\n    label\n    valueLabel\n    currencyCode\n    __typename\n  }\n  alternateRentFrequencyPrice {\n    internalValue\n    rentFrequencyLabel\n    unitsLabel\n    label\n    valueLabel\n    currencyCode\n    __typename\n  }\n  __typename\n}\n\nfragment ENERGY_PERFORMANCE_CERTIFICATE on ListingData {\n  epc {\n    image {\n      caption\n      filename\n      __typename\n    }\n    pdf {\n      caption\n      original\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment LISTING_FEATURES on ListingData {\n  detailedDescription\n  features {\n    ...LISTING_FEATURES_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment LISTING_FEATURES_FRAGMENT on Features {\n  bullets\n  flags {\n    furnishedState {\n      name\n      label\n      __typename\n    }\n    studentFriendly\n    tenure {\n      name\n      label\n      __typename\n    }\n    availableFromDate\n    __typename\n  }\n  highlights {\n    description\n    label\n    __typename\n  }\n  __typename\n}\n\nfragment FLOOR_PLANS on ListingData {\n  floorPlan {\n    ...FLOOR_PLANS_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment FLOOR_PLANS_FRAGMENT on FloorPlan {\n  image {\n    filename\n    caption\n    __typename\n  }\n  links {\n    url\n    label\n    __typename\n  }\n  pdf {\n    original\n    caption\n    __typename\n  }\n  __typename\n}\n\nfragment FLOOR_AREA on ListingData {\n  floorArea {\n    ...FLOOR_AREA_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment FLOOR_AREA_FRAGMENT on FloorArea {\n  label\n  range {\n    maxValue\n    maxValueLabel\n    minValue\n    minValueLabel\n    __typename\n  }\n  units\n  unitsLabel\n  value\n  __typename\n}\n\nfragment MEDIA on ListingData {\n  content {\n    virtualTour {\n      ...MEDIA_FRAGMENT\n      __typename\n    }\n    floorPlan {\n      ...MEDIA_FRAGMENT\n      __typename\n    }\n    audioTour {\n      ...MEDIA_FRAGMENT\n      __typename\n    }\n    __typename\n  }\n  propertyImage {\n    ...MEDIA_FRAGMENT\n    __typename\n  }\n  additionalLinks {\n    ...MEDIA_FRAGMENT\n    ... on AdditionalLink {\n      href\n      label\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment MEDIA_FRAGMENT on Media {\n  original\n  caption\n  url\n  filename\n  type\n  __typename\n}\n\nfragment MAP on ListingData {\n  location {\n    ...LISTING_LOCATION_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment LISTING_LOCATION_FRAGMENT on ListingLocation {\n  coordinates {\n    isApproximate\n    latitude\n    longitude\n    __typename\n  }\n  postalCode\n  streetName\n  countryCode\n  propertyNumberOrName\n  townOrCity\n  __typename\n}\n\nfragment EMBEDDED_CONTENT on ListingData {\n  embeddedContent {\n    videos {\n      ...MEDIA_FRAGMENT\n      __typename\n    }\n    tours {\n      ...MEDIA_FRAGMENT\n      __typename\n    }\n    links {\n      ...MEDIA_FRAGMENT\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment POINTS_OF_INTEREST on ListingData {\n  pointsOfInterest {\n    ...POINTS_OF_INTEREST_FRAGMENT\n    __typename\n  }\n  __typename\n}\n\nfragment POINTS_OF_INTEREST_FRAGMENT on PointOfInterest {\n  title\n  address\n  type\n  latitude\n  longitude\n  distanceMiles\n  __typename\n}\n\nfragment PRICE_HISTORY on ListingData {\n  priceHistory {\n    firstPublished {\n      firstPublishedDate\n      priceLabel\n      __typename\n    }\n    lastSale {\n      date\n      newBuild\n      price\n      priceLabel\n      recentlySold\n      __typename\n    }\n    priceChanges {\n      isMinorChange\n      isPriceDrop\n      isPriceIncrease\n      percentageChangeLabel\n      priceChangeDate\n      priceChangeLabel\n      priceLabel\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment LISTING_SUMMARY on ListingData {\n  listingId\n  displayAddress\n  category\n  location {\n    postalCode\n    streetName\n    uprn\n    __typename\n  }\n  section\n  branch {\n    logoUrl\n    name\n    __typename\n  }\n  counts {\n    numBedrooms\n    numBathrooms\n    numLivingRooms\n    __typename\n  }\n  featurePreview {\n    iconId\n    content\n    __typename\n  }\n  floorArea {\n    label\n    range {\n      maxValue\n      maxValueLabel\n      minValue\n      minValueLabel\n      __typename\n    }\n    units\n    unitsLabel\n    value\n    __typename\n  }\n  imagePreview {\n    caption\n    src\n    __typename\n  }\n  propertyImage {\n    caption\n    original\n    __typename\n  }\n  listingUris {\n    contact\n    detail\n    __typename\n  }\n  pricing {\n    label\n    internalValue\n    __typename\n  }\n  tags {\n    label\n    __typename\n  }\n  title\n  transports {\n    distanceInMiles\n    poiType\n    title\n    __typename\n  }\n  publicationStatus\n  publishedOn\n  numberOfImages\n  statusSummary {\n    label\n    __typename\n  }\n  ...LISTING_ANALYTICS_TAXONOMY\n  __typename\n}"
    }
    print('--> start', listing_id)
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            headers['User-Agent'] = random.choice(ua_list)
            resp = requests.post(
                url=listing_details_api, 
                headers=headers, 
                data=json.dumps(payload), 
                verify=False, 
                timeout=10, 
                proxies=proxies
            )
            if resp.status_code == 200:
                print('--> end', listing_id)
                json_details = json.loads(resp.text)
                if json_details and 'data' in json_details:
                    page_data.append(json_details)
                    return True
                logging.warning(f"Invalid response format for listing {listing_id}")
                return False
        except Exception as e:
            logging.error(f"Error fetching listing {listing_id}: {str(e)}")
            retry_count += 1
            time.sleep(random.randint(5, 10))
    
    logging.error(f"Failed to fetch listing {listing_id} after {max_retries} retries")
    return False

def fetch_listing(district):
    global page_data

    page_number = 1 
    identifier = district.replace(' ', '-')
    value = district.replace(' ', '%20')
    has_listings = False

    while True:
        url = 'https://www.zoopla.co.uk/api/search/mobile/?excludeRadius=true&location.identifier={}&location.value={}&newHomes=include&page={}&section=for-sale&sortOrder=newest_listings'.format(identifier, value, page_number)
       
        
        try:
            api_response = curl_request.get(
                url=url,
                headers={
                    'x-api-key': 'ipqd2r4biqve2yh49sjxaujvpppey8s',
                    'Host': 'www.zoopla.co.uk',
                    'User-Agent': random.choice(ua_list)
                },
                verify=False,
                timeout=30,
                impersonate="safari",
            )
            
            if api_response.status_code == 404:
                break
                
            if api_response.status_code != 200:
                logging.error(f"Failed to fetch district {district}: Status {api_response.status_code}")
                break

            json_response = json.loads(api_response.text).get('data', '')
            if not json_response or not json_response['listings']['regular']:
                break

            has_listings = True
            thread_list = []
            threads_count = 5
            listings = json_response['listings']['regular']
            thread_loop = (len(listings) + threads_count - 1) // threads_count

            page_data = []
            for loop in range(thread_loop):
                start_idx = loop * threads_count
                end_idx = min((loop + 1) * threads_count, len(listings))
                current_batch = listings[start_idx:end_idx]
                
                threads = []
                for _property in current_batch:
                    listing_id = _property['listingId']
                    thread = Thread(target=fetch_listing_details, args=(listing_id,))
                    threads.append(thread)
                    thread.start()
                    

                for t in threads:
                    t.join()

            for _property in listings:
                matching_details = [v for v in page_data if v.get('data', {}).get('listingDetails', {}).get('listingId') == _property['listingId']]
                if matching_details:
                    _property['detail_page'] = matching_details[0]
                else:
                    _property['detail_page'] = None
                    logging.warning(f"No details found for listing {_property['listingId']}")

            item = {
                'page_number': page_number,
                'district': district,
                'all_information': json_response
            }
            all_data.append(item)
            page_number += 1
            
            # Add delay between pages to avoid detection
            time.sleep(random.uniform(1.5, 3.0))

        except Exception as e:
            logging.error(f"Error processing district {district}: {str(e)}")
            break

    return has_listings

def fetch_districts(file):
    try:
        print(f"Attempting to read CSV file: {file}")
        df = pd.read_csv(file)
        print(f"Successfully read CSV file. Column names: {df.columns.tolist()}")
        
        # Use the 'Postcode' column instead of 'District'
        districts = df['Postcode'].str.strip().unique().tolist()
        print(f"Found {len(districts)} unique districts")
        if len(districts) > 0:
            print(f"First few districts: {districts[:5]}")
        
        logging.info(f"Loaded {len(districts)} valid postcode districts")
        return districts
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        logging.error(f"Error loading districts: {str(e)}")
        return []

def process_district_batch(districts, processed_districts, pbar):
    skipped = []
    district_counter = 0
    districts_list = districts[:1]
    for district in districts_list:
        print(f'The District index is : {district_counter} from {len(districts_list)}',  )
        district_counter += 1
        if district in processed_districts:
            pbar.update(1)
            continue
            
        has_listings = fetch_listing(district)
        if not has_listings:
            skipped.append(district)
            
        processed_districts.append(district)
        pbar.update(1)
        
        # Save progress periodically
        if len(processed_districts) % SAVE_INTERVAL == 0:
            save_checkpoint(processed_districts, all_data)
            
    return skipped

if __name__ == '__main__':
# def Main():
    global all_data
    all_data = list()
    

    # Load existing data if any
    all_data = load_temp_data()
    processed_districts = load_checkpoint()
    logging.info(f"Loaded {len(processed_districts)} processed districts from checkpoint")

    # INPUT FILE NAME 
    input_file = r'D:\scrapy_projects\current_projects\scrapy_projects\solomon\airflow\include\zoopla_resources\Postcode_districts.csv'
    districts = fetch_districts(input_file)
    
    # Remove already processed districts
    remaining_districts = [d for d in districts if d not in processed_districts]
    
    skipped_districts = []
    
    with tqdm(total=len(remaining_districts), desc="Processing districts", ncols=100) as pbar:
        # Process districts in batches
        for i in range(0, len(remaining_districts), BATCH_SIZE):
            batch = remaining_districts[i:i + BATCH_SIZE]
            skipped = process_district_batch(batch, processed_districts, pbar)
            skipped_districts.extend(skipped)
            
            # Add random delay between batches
            time.sleep(random.uniform(2.0, 4.0))

    # Save skipped districts
    with open(SKIPPED_DISTRICTS_FILE, 'w') as f:
        f.write('\n'.join(skipped_districts))
    logging.info(f"Saved {len(skipped_districts)} skipped districts to {SKIPPED_DISTRICTS_FILE}")

    # OUTPUT FILE NAME

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(all_data, f)
    logging.info(f"Data saved to {OUTPUT_FILE}")

    # Clean up temp files
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    if os.path.exists(TEMP_DATA_FILE):
        os.remove(TEMP_DATA_FILE)

