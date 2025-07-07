# 🏡 Zoopla Property Data Pipeline (Airflow DAG)

This repository contains a fully automated data pipeline built with **Apache Airflow** to extract, clean, analyze, and report real estate listings from **Zoopla**.  
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
     ┌────────────┐
     │   Spider   │ ←── Scrapy scraper (Zoopla)
     └────┬───────┘
          ↓
   [MongoDB: Raw_Data]
          ↓
     Cleaning Step
          ↓
[Elasticsearch: stage_2_clean]
          ↓
     Comparison Step ──▶ Add New Records
        │              ──▶ Flag Deleted Records
        └────────────▶ Update Existing Records
          
          ↓
   Analytics Generation
        ↓
[Elasticsearch: stage_4_analysis]
        ↓
[Google Sheets Report]
```

---

## 📅 DAG Schedule

- **Runs Weekly**  
- **CRON:** `30 12 * * 0` → Every **Sunday at 12:30 PM UTC**

---

## 🛠 Setup & Deployment

> 🔒 **Note:** All credentials (Slack webhook, MongoDB, ES, Sheets API) are managed via Airflow **Connections/Variables**. No secrets are stored in this code.

1. Clone the repo:
   ```bash
   git clone https://github.com/your-username/zoopla-pipeline.git
   ```

2. Add environment connections in Airflow UI:
   - `slack_webhook`
   - `mongo_conn`
   - `es_conn`
   - `google_sheets_conn`

3. Deploy the DAG to your Airflow environment (Astro, Local, or Cloud).

---

## 📊 Output

- **Elasticsearch Indices**
  - `prod_zoopla_stage_2_clean`
  - `prod_zoopla_stage_3_properties_all`
  - `prod_zoopla_stage_4_analysis`

- **Google Sheet Report**
  - Daily snapshot of analysis results

---

## 📸 Screenshots

> *(Optional: Add screenshots of your Airflow DAG or Google Sheet report)*

---

## 🤝 About the Author

**👨‍💻 Ramez Rasmy**  
Freelance Data Engineer | Airflow, Python, Docker, Scraping  
📫 Contact: [Upwork Profile]([https://www.upwork.com/freelancers/~yourprofile](https://upwork.com/freelancers/ramezr))

