📈 TradeStream Analytics: End-to-End Financial News & Sentiment Pipeline

An automated, end-to-end data engineering and sentiment analysis pipeline that ingests live market and global economic news from Finnhub.io, processes text sentiment using VADER NLP, manages incremental data persistence in an Excel Master Store on Google Drive, and serves interactive analytics via Power BI.

🔗 Quick Links

📊 Live Power BI Dashboard: Interactive Financial News Analytics Dashboard

🐍 Execution Engine: Google Colab Notebook

🏗️ Architecture & Data Workflow

[ Finnhub.io REST API ]
         │ (Raw Financial News Data)
         ▼
[ Google Colab Engine ]
    ├── 1. Data Ingestion (finnhub-python SDK)
    ├── 2. Data Cleaning & UNIX Timestamp Formatting
    ├── 3. NLP Sentiment Analysis (vaderSentiment)
    └── 4. Incremental Appending & Deduplication (pandas / openpyxl)
         │
         ▼
[ Google Drive Storage ] ───► Daily_news_master.xlsx
         │
         ▼ (Data Refresh)
[ Power BI Visual Analytics Dashboard ]


✨ Key Features

Real-Time Financial Ingestion: Connects directly to Finnhub's REST API using the official finnhub-python client to pull general market and macro-economic updates.

Compound Text Sentiment Analysis: Combines headline and summary text streams to run rich NLP evaluations using vaderSentiment, categorizing market sentiment into Positive, Neutral, or Negative.

Smart Master File Management: Automatically detects existing master files on Google Drive (Daily_news_master.xlsx), appends new records, and drops duplicate news items based on unique article IDs (subset=['id']).

Interactive Power BI Reporting: Connects Excel master records to Power BI desktop/service to track market news distributions, news volume, and overall market sentiment index over time.

🛠️ Tech Stack & Dependencies

Tool / Library

Role

Python 3.x

Core pipeline execution language

Google Colab

Cloud notebook interface for running scheduled/ad-hoc ingestion

finnhub-python

Official Python wrapper for the Finnhub Financial API

vaderSentiment

Valence Aware Dictionary and sEntiment Reasoner for NLP scoring

pandas

Data cleaning, structure transformation, deduplication, and manipulation

openpyxl

Underlying engine for read/write operations on .xlsx files

Google Drive API

Cloud file storage and persistent master database host

Power BI

Business Intelligence & Visual Analytics dashboard layer

🔎 Step-by-Step Pipeline Walkthrough

1. Library Installation & Environment Setup

Initializes required third-party libraries inside Google Colab:

!pip install finnhub-python vaderSentiment openpyxl --quiet


2. Finnhub API Client & Google Drive Mounting

Connects to the Finnhub API service and mounts Google Drive storage for master file storage:

import os, datetime
import pandas as pd
import finnhub
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from google.colab import drive

# Mount Google Drive
drive.mount('/content/drive')

# Initialize Finnhub Client
FINNHUB_API_KEY = "YOUR_FINNHUB_API_KEY"
client = finnhub.Client(api_key=FINNHUB_API_KEY)

# Define File Directories
folder_path = '/content/drive/My Drive/Financial_News_Pipeline'
os.makedirs(folder_path, exist_ok=True)
excel_file_path = os.path.join(folder_path, 'Daily_news_master.xlsx')


3. Data Ingestion & Field Selection

Pulls general news feeds from Finnhub, isolates critical fields, and standardizes time formats:

news_list = client.general_news('general', min_id=0)
df_raw = pd.DataFrame(news_list)

# Select relevant columns
columns_to_keep = ['id', 'datetime', 'headline', 'summary', 'source', 'category', 'url']
existing_cols = [col for col in columns_to_keep if col in df_raw.columns]
df = df_raw[existing_cols].copy()

# Convert UNIX timestamp to human-readable datetime
df['datetime'] = pd.to_datetime(df['datetime'], unit='s')
df['date'] = df['datetime'].dt.date


4. Text Preparation & VADER Sentiment Analysis

Combines text fields for composite NLP evaluation and assigns categorical sentiment labels:

# Clean text and create rich context input
df['headline'] = df['headline'].fillna('').astype(str).str.strip()
df['summary'] = df['summary'].fillna('').astype(str).str.strip()
df['text_for_sentiment'] = df['headline'] + ". " + df['summary']

# Analyze sentiment
analyzer = SentimentIntensityAnalyzer()
df['sentiment_score'] = df['text_for_sentiment'].apply(
    lambda x: analyzer.polarity_scores(x)['compound']
)

# Categorize threshold scores
df['sentiment_label'] = df['sentiment_score'].apply(
    lambda score: 'Positive' if score > 0.05 else ('Negative' if score < -0.05 else 'Neutral')
)
df = df.drop(columns=['text_for_sentiment'])


5. Incremental Archiving & Deduplication

Appends newly fetched news records into the master dataset stored on Google Drive while eliminating redundant records:

if os.path.exists(excel_file_path):
    existing_df = pd.read_excel(excel_file_path)
    combined_df = pd.concat([existing_df, df], ignore_index=True)
    
    # Deduplicate based on unique article 'id'
    combined_df.drop_duplicates(subset=['id'], keep='last', inplace=True)
    combined_df.to_excel(excel_file_path, index=False)
else:
    df.to_excel(excel_file_path, index=False)


📊 Power BI Dashboard & Visual Insights

The output dataset stored in Daily_news_master.xlsx feeds directly into the interactive Power BI dashboard.

🌐 Live Dashboard Access

👉 Click Here to View the TradeStream Analytics Dashboard

Key Metrics & Dashboard Features

News Volume KPI Cards: Real-time metrics highlighting total ingested news stories, average daily sentiment, and total active coverage sources.

Sentiment Distribution (Positive vs. Neutral vs. Negative): Visual breakdown displaying market sentiment polarization across headlines.

Publisher Breakdown: Top financial news sources sorted by publication volume.

Time Series Trend Analysis: Interactive line graphs tracking shifting sentiment trends across custom date ranges.

Interactive Headline Explorer: Tabular feed with direct URL drill-throughs to original news source links.

🚀 Setup & Execution Guide

Prerequisites

Free API Key from Finnhub.io.

A Google account with Google Colab and Google Drive access.

Power BI Desktop (for local dataset editing or republishing).

Execution Steps

Open Google Colab and paste the script code.

Replace FINNHUB_API_KEY with your personal key.

Run the script cell. Authorize Google Drive mounting when prompted.

Verify that Daily_news_master.xlsx has been generated under Financial_News_Pipeline in your Google Drive.

Open Power BI, set your source path to Daily_news_master.xlsx (or link via Google Drive sync), and click Refresh.

🛣️ Future Scope & Roadmap

[ ] Automated Cron Execution: Schedule notebook execution via GitHub Actions or Google Cloud Functions.

[ ] Ticker-Level Entity Extraction: Expand from general market news to company-specific ticker feeds (e.g., AAPL, TSLA, MSFT).

[ ] Advanced Transformer NLP: Upgrade from VADER to fine-tuned Financial LLMs/Transformers (e.g., FinBERT) for higher sentiment precision.

[ ] Automated Cloud Warehouse Load: Transition storage from Excel files to BigQuery or PostgreSQL.