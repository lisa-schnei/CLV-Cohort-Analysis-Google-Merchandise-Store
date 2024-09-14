
# Checking time frame of the data
-- Min: 2020-11-01, Sunday
-- Max: 2021-01-31, Sunday
SELECT
min_date,
EXTRACT(DAYOFWEEK FROM min_date) AS min_day,
max_date,
EXTRACT(DAYOFWEEK FROM max_date) AS max_day,
FROM (SELECT 
   MIN(TIMESTAMP_MICROS(event_timestamp)) AS min_date, 
   MAX(TIMESTAMP_MICROS(event_timestamp)) AS max_date
   FROM `turing_data_analytics.raw_events`);


# Checking how many events are purchase events
-- 5,692 purchase events, some having NULL values in event_value_in_usd, need further investigation
SELECT *
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase';

-- only purchase events have values in purchase_revenue_in_usd
SELECT 
SUM(purchase_revenue_in_usd) AS total
FROM `turing_data_analytics.raw_events`
WHERE event_name != 'purchase'

# Counting NULL values and duplicates in relevant columns
-- 450 events missing event_value; however, no missing records for purchase_revenue_in_usd
-- 4,419 distinct user_pseudo_id of 5,692 total events
-- 450 missing quantity and tax_value
-- 23 missing transaction_id
SELECT 
COUNT(*) AS total_rows,
SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) AS timestamp_null,
SUM(CASE WHEN event_value_in_usd IS NULL THEN 1 ELSE 0 END) AS event_value_null,
SUM(CASE WHEN user_pseudo_id IS NULL THEN 1 ELSE 0 END) AS id_null,
COUNT(DISTINCT user_pseudo_id) AS id_distinct,
SUM(CASE WHEN total_item_quantity IS NULL THEN 1 ELSE 0 END) AS qty_null,
SUM(CASE WHEN purchase_revenue_in_usd IS NULL THEN 1 ELSE 0 END) AS purch_rev_null,
SUM(CASE WHEN refund_value_in_usd IS NULL THEN 1 ELSE 0 END) AS refund_value_null,
SUM(CASE WHEN shipping_value_in_usd IS NULL THEN 1 ELSE 0 END) AS shipping_value_null,
SUM(CASE WHEN tax_value_in_usd IS NULL THEN 1 ELSE 0 END) AS tax_value_null,
SUM(CASE WHEN transaction_Id IS NULL THEN 1 ELSE 0 END) AS transaction_id_null
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase';

# Checking how the different value and quantity columns are related

SELECT
user_pseudo_id,
event_value_in_usd,
total_item_quantity,
purchase_revenue_in_usd,
tax_value_in_usd
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase';

-- event_value_in_usd is equal to purchase_value_in_usd
SELECT 
SUM(CASE WHEN event_value_in_usd != purchase_revenue_in_usd THEN 1 ELSE 0 END) AS unequal_values
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase';

-- All purchases with NULL in event_value_in_Usd have 0 in purchase value.This may be due to data errors and I will exclude them from the analysis. 
SELECT *,
SUM(purchase_revenue_in_usd) OVER() AS total_purch_value
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase'
  AND event_value_in_usd IS NULL;


# Calculating required metrics - AOV, purchase frequency, custoner value and CLV
-- 5,242 purchase events with valid purchase values.
SELECT
  id_distinct,
  AOV,
  purch_frequency,
  (AOV * purch_frequency) AS customer_value,
  (AOV * purch_frequency * 3) AS CLV
  FROM (SELECT 
          COUNT(DISTINCT user_pseudo_id) AS id_distinct, # 4,066 distinct customers    
          SUM(purchase_revenue_in_usd) / COUNT(event_name) AS AOV,
          COUNT(event_name) / COUNT(DISTINCT user_pseudo_id) AS purch_frequency
    FROM `turing_data_analytics.raw_events`
    WHERE event_name = 'purchase'
    AND event_value_in_usd IS NOT NULL);


----------------------------------

# Checking number of first_visit events
-- 257,462 records total with first_visit
-- 257,314 unique user_pseudo_ids, meaning some users must have multiple first_visit events
SELECT COUNT(*) AS total_events,
COUNT(DISTINCT user_pseudo_id) AS distinct_users
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'first_visit'

# Investigating users with multiple first_visit events
-- events are recorded on different days, could be due to incorrect tracking, privacy mode or similar. Should therefore probably use the earliest first_visit events per user
WITH multiple_first_visits AS (
    SELECT user_pseudo_id
    FROM `turing_data_analytics.raw_events`
    WHERE event_name = 'first_visit'
    GROUP BY user_pseudo_id
    HAVING COUNT(*) > 1
)
SELECT *
FROM `turing_data_analytics.raw_events`
WHERE user_pseudo_id IN (SELECT user_pseudo_id FROM multiple_first_visits)
ORDER BY user_pseudo_id, event_timestamp;

# Checking viability of user_first_touchtimestamp
SELECT *
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'first_visit'
AND event_timestamp != user_first_touch_timestamp

# Creating cohorts and number of first_visits per cohort

WITH cohort_weeks AS (
  SELECT user_pseudo_id,
  # Truncating event_timestamp, selecting minimum to eliminate duplicate first_visit events and formatting to DATE
  DATE(DATE_TRUNC(TIMESTAMP_MICROS(MIN(event_timestamp)), WEEK)) AS week_start,
  FROM `turing_data_analytics.raw_events`
  #WHERE event_name = 'first_visit' - removing first_visit requirement and just going by first recorded event per user
  GROUP BY 1
)

SELECT 
week_start,
COUNT(DISTINCT cohort_weeks.user_pseudo_id) AS registrations,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = cohort_weeks.week_start THEN raw.purchase_revenue_in_usd END) AS week_0,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 1 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_1,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 2 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_2,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 3 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_3,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 4 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_4,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 5 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_5,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 6 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_6,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 7 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_7,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 8 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_8,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 9 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_9,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 10 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_10,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 11 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_11,
SUM(CASE WHEN DATE(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) = DATE_ADD(cohort_weeks.week_start, INTERVAL 12 WEEK) THEN raw.purchase_revenue_in_usd END) AS week_12
FROM cohort_weeks AS cohort_weeks
LEFT JOIN `turing_data_analytics.raw_events` AS raw
ON cohort_weeks.user_pseudo_id = raw.user_pseudo_id
GROUP BY 1
ORDER BY 1

# Table for data validation
SELECT
user_pseudo_id,
DATE(DATE_TRUNC(TIMESTAMP_MICROS(MIN(event_timestamp)), WEEK)) AS week_start,
DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_time,
event_name,
purchase_revenue_in_usd
FROM `turing_data_analytics.raw_events`
GROUP BY ALL
ORDER BY 1, 3





