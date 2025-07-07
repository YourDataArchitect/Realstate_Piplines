<p align="center">
  <img src="https://logonews.fr/wp-content/uploads/2023/12/seloger_thumbnail_logonews.png" alt="SeLoger Logo" width="400"/>
</p>


# 🏡 Seloger Property Data Pipeline (Airflow DAG)

This repository contains a fully automated data pipeline built with **Apache Airflow** to extract, clean, analyze, and report real estate listings from **Seloger**.  
It pushes data to **MongoDB**, **Elasticsearch**, and **Google Sheets**, with real-time Slack alerts for monitoring.

---

## 🚀 Features

✅ **Scheduled weekly** to collect and process new listings  
✅ Cleans raw data and syncs with existing records  
✅ Tracks **new**, **deleted**, and **updated** listings  
✅ Analyzes market statistics (prices, turnover, etc.)  
✅ Sends **Slack alerts** for start, success, and failure  
✅ Delivers analytics to **Elasticsearch** and **Google Sheets**

---

## 🧠 Technologies Used

- Apache Airflow 2 / 3 (Astro Runtime)
- Python
- MongoDB
- Elasticsearch
- Google Sheets API
- Slack Webhook Integration
- Scrapy

---

## 📦 Pipeline Architecture
```
                 ┌────────────────────────────┐
                 │      DAG Starts            │
                 └────────────┬───────────────┘
                              ↓
                    [Slack: Notify_Start]
                              ↓
                 ┌────────────────────────────┐
                 │  Delete_Old_Records        │
                 │  (MongoDB Raw_Data)        │
                 └────────────┬───────────────┘
                              ↓
                 ┌────────────────────────────┐
                 │  Running_Seloger_Spider    │
                 │  (Scrapy → JSON Output)    │
                 └────────────┬───────────────┘
                              ↓
                 ┌────────────────────────────┐
                 │  Cleaning_Data_Validation  │
                 │  (To ES: stage_2_clean)     │
                 └────────────┬───────────────┘
                              ↓
                 ┌────────────────────────────┐
                 │      Compare_Data          │
                 │(stage_2_clean vs stage_3)  │
                 └────┬────────┬────────┬─────┘
                      │        │        │
          ┌───────────┘        │        └─────────────┐
          ↓                    ↓                      ↓
 [Add_New_Records]   [Flag_Delete_Records]   [Update_Common_Records]
          ↓                    ↓                      ↓
          └────────────┬───────┴────────────┬────────┘
                       ↓                    ↓
                ┌────────────────────────────┐
                │     Analyse_Records        │
                │ (Generate analytics data)  │
                └────┬──────────────┬────────┘
                     ↓              ↓
         [Feed_Analytics_ES]   [Feed_Google_Sheet]
                     ↓              ↓
                     └────┬─────────┘
                          ↓
                [Slack: Notify_End]
                          ↓
                [Slack: notify_failure]
```


---

## 📅 DAG Schedule

- **Runs Weekly**  
- **CRON:** `30 12 * * 0` → Every **Sunday at 12:30 PM UTC**

---

## 📊 Output

- **Elasticsearch Indices**
  - `stage_2_clean`: Contains data after cleaning and validation — *temporary index*
  - `stage_3_properties_all`: Contains all processed records ready for analysis — *permanent index*
  - `stage_4_analysis`: Contains analytical metrics for the French real estate market — *permanent index*


- **Google Sheet Report**
  - Daily snapshot of analysis results

---

## 🤝 About the Author

**👨‍💻 Ramez Rasmy**  
Freelance Data Engineer | Airflow, Python, Docker, Scraping  
📫 Contact: [Upwork Profile]([https://www.upwork.com/freelancers/~yourprofile](https://upwork.com/freelancers/ramezr))

