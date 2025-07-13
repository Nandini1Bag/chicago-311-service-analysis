# 📊 Chicago 311 City Service Request Analysis

# 📘 Project Overview

# This project analyzes the City of Chicago’s 311 non-emergency service request data (2021–2023).

# The goal is to:

# 

# Identify trends in service requests

# 

# Evaluate request response times

# 

# Understand communication methods used by residents

# 

# Visualize geographic distribution of requests by neighborhood

# 

# 📌 Impact: These insights can help city departments better prioritize resources, improve efficiency, and enhance overall service delivery.

# 

# 🔗 Data Source

# 📂 Dataset: Chicago 311 Service Requests

# Source: City of Chicago Open Data Portal

# 

# 🗂 Project Structure

# bash

# Copy

# Edit

# chicago-311-service-analysis/

# │

# ├── data/             # Contains raw and processed datasets

# ├── notebooks/        # Jupyter notebooks for EDA and visualization

# │   └── analysis.ipynb

# ├── scripts/          # Python scripts for fetching data

# │   └── fetch\_data.py

# ├── reports/          # Final project report or slides (optional)

# ├── requirements.txt  # Python dependencies

# └── README.md         # Project overview and instructions

# 🛠️ How to Run This Project

# 1\. Clone the Repository

# bash

# Copy

# Edit

# git clone https://github.com/yourusername/chicago-311-service-analysis.git

# cd chicago-311-service-analysis

# 2\. Set Up Virtual Environment

# bash

# Copy

# Edit

# python -m venv venv

# \# Activate virtual environment:

# \# On macOS/Linux:

# source venv/bin/activate

# \# On Windows:

# venv\\Scripts\\activate

# 3\. Install Dependencies

# bash

# Copy

# Edit

# pip install -r requirements.txt

# 4\. Fetch the Data

# bash

# Copy

# Edit

# python scripts/fetch\_data.py

# 5\. Run the Analysis Notebook

# bash

# Copy

# Edit

# jupyter notebook notebooks/analysis.ipynb

# 📈 Key Analyses Performed

# 📅 Time Series Analysis: Trends in service requests (monthly, yearly)

# 

# ⏱️ Response Time Evaluation: Duration between request and resolution

# 

# ☎️ Communication Channel Usage: (Phone, web, mobile app, etc.)

# 

# 🗺️ Geographic Mapping: Requests by community area and neighborhood

# 

# 🔍 Insights \& Recommendations

# High-demand request types and peak periods identified for resource allocation.

# 

# Neighborhoods with longer response times were flagged for follow-up.

# 

# Phone and internet identified as the most used communication tools.

# 

# 🚀 Future Work

# Add NLP sentiment analysis from resident feedback (if available)

# 

# Build an interactive dashboard using Streamlit or Tableau

# 

# Use machine learning models to forecast high-demand periods

# 

# 💼 Skills \& Tools Used

# Languages: Python

# 

# Libraries: pandas, matplotlib, seaborn, requests, datetime

# 

# Platform: Jupyter Notebook

# 

# Data Source: REST API from data.cityofchicago.org

# 



