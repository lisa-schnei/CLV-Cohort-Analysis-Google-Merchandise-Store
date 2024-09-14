# Project: CLV Cohort Analysis - Google Merchandise Store

--------------------------------------------
## Project Objective

Working with a dataset for the Google Merchandise Store, objective of this project was to conduct a CLV analysis for weekly cohorts. This analysis looks at revenue trends per cohort and predicted revenue per cohort. 

**Questions this project aimed to answer:**
1. How does the revenue per site visitor develop over time for each cohort?
2. What trends can be seen between cohorts and over time?
3. What is the cumulative revenue we get per cohort (CLV)?
4. What CLV can we predict for each cohort based on known data?

**Tools used**
SQL in BigQuery, Google Sheets


## Project Content

**BigQuery CLV Analysis - Exploratory analysis.sql** - SQL file containing the exploratory analysis and data cleaning/ preparation steps taken in BigQuery to retrieve the required data from the dataset

[**Project Workbook**](https://docs.google.com/spreadsheets/d/17Nf_o8FO7RxxSYTbe6wJ0ckLyWxJ1mKymyS5-ghZYi8/edit?usp=sharing) - Google Sheets workbook containing project overview, investigation, SQL code and final cohort tables


## Data & Context

**Data Source:** [Turing College raw_events table](https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1stc-da-1!2sturing_data_analytics!3sraw_events)

**Data Timeframe:** 
Start: 2020-11-01 
End: 2021-01-31

**Metrics:**
- Registrations = number of unique site visitors per cohort (based on user_pseudo_id). Cohort week is assigned based on the earliest tracked event for each site visitor.
- Weekly Revenue = revenue per cohort and week, based on purchase event and purchase_revenue_in_usd data
- Weekly Average Revenue = weekly revenue per cohort / cohort registrations
- Cumulative Revenue = previous weeks' revenue + current week revenue
- Cumulative Average Revenue = cumulative revenue per cohort / cohort registrations
- Predicted Revenue per Cohort = previous weeks' average revenue * (1 + cumulative growth rate for the week)

