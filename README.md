# 🏠 Zoopla Property Pipeline — Airflow DAG

Automate your **end‑to‑end Zoopla data ingestion** with a single, production‑ready Apache Airflow DAG.  
This pipeline **scrapes Zoopla listings weekly**, cleans and enriches the data, and keeps your Elasticsearch index perfectly in sync.

<p align="center">
  <img src="https://img.shields.io/badge/Airflow-2.x-blue?logo=apacheairflow" alt="Airflow 2.x">  
  <img src="https://img.shields.io/badge/Scrapy-2.x-green?logo=scrapy" alt="Scrapy 2.x">  
  <img src="https://img.shields.io/badge/Elastic-%E2%9A%92%EF%B8%8F Search-yellow?logo=elasticsearch" alt="Elasticsearch">
</p>

---

## ✨ Key Features

| Stage | Task ID | What it does |
|-------|---------|--------------|
| **1. Scrape** | `Running_Zoopla_Spider` | Runs a Scrapy spider that crawls Zoopla listings and writes `zoopla_listing_data.json` |
| **2. Wait** | `Wait_Spider_Finish` | FileSensor politely waits until the JSON export is ready |
| **3. Clean** | `Clean_Data` | Normalises, deduplicates, and converts JSON ➜ CSV (`pandas` under the hood) |
| **4. Compare** | `Compare_Data` | Compares fresh CSV with Elasticsearch to find **new** and **deleted** records |
| **5. Insert** | `Add_New_Records` | Bulk‑inserts brand‑new listings |
| **6. Update/Delete** | `Update_Delete_Records` | Marks outdated listings as inactive or removes them |

<details>
<summary>Task Graph</summary>

```mermaid
graph LR
  A[Running_Zoopla_Spider] --> B[Wait_Spider_Finish]
  B --> C[Clean_Data]
  C --> D[Compare_Data]
  D --> E[Add_New_Records]
  D --> F[Update_Delete_Records]
