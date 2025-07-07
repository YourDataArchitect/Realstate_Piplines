
![Picture of the ISS](https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTMD17nUoqektiX0JwVGfM704YUL6EOIDkZSg&s)

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
