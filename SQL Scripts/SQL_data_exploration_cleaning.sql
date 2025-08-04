-- E-Commerce End to End Project
-- First leg of the Project of Data Manipulation and Cleaning will be in PostgreSQL.

-- Creating table customers
DROP TABLE IF EXISTS customers;
Create Table customers 
(
	customer_id		INT PRIMARY KEY,
	full_name		VARCHAR(100),
	age				INT,
	city			VARCHAR(50),
	gender			VARCHAR(10),
	state			VARCHAR(50),
	yearly_income	DECIMAL(10,2),
	education		VARCHAR(50),
	occupation		VARCHAR(50)		
);

-- Copy the data from csv file to customers table
COPY customers(customer_id, full_name, age, city, gender, state, yearly_income, education, occupation)
FROM 'C:\\Program Files\\PostgreSQL\\17\\data\\customers.csv'
DELIMITER ','
CSV HEADER;

-- Query customers table
SELECT *
FROM customers
LIMIT 5;

-- Creating transactions table
DROP TABLE IF EXISTS transactions;
Create Table transactions 
(
	transaction_id		INT PRIMARY KEY,
	session_id			INT,
	customer_id			INT,
	purchase_amount		DECIMAL(10, 2),
	product_category	VARCHAR(50),
	payment_method		VARCHAR(50),
	transaction_date	TIMESTAMP 
);

-- Adding data to transactions table from csv file
COPY transactions(transaction_id, session_id, customer_id, purchase_amount, product_category, payment_method,	transaction_date)
FROM 'C:\\Program Files\\PostgreSQL\\17\\data\\transactions.csv'
DELIMITER ','
CSV HEADER;

-- Query first 5 rows of transactions table
SELECT *
FROM transactions
LIMIT 5;

-- Creating churn table
DROP TABLE IF EXISTS churn;
Create Table churn 
(
	customer_id					INT,
	total_orders				INT,
	days_since_last_purchase	INT,
	last_purchase_date			TIMESTAMP
);

-- Adding data to transactions table from csv file
COPY churn(customer_id, total_orders, days_since_last_purchase, last_purchase_date)
FROM 'C:\\Program Files\\PostgreSQL\\17\\data\\churn.csv'
DELIMITER ','
CSV HEADER;

-- Query first 5 rows of churn table
SELECT *
FROM churn
LIMIT 5;

-- Creating session_events table
DROP TABLE IF EXISTS session_events;
Create Table session_events 
(
	session_id	INT,
	customer_id	INT,
	event_type	VARCHAR(20),
	timestamp	TIMESTAMP
);

-- Adding data to session_events table from csv file
COPY session_events(session_id, customer_id, event_type, timestamp)
FROM 'C:\\Program Files\\PostgreSQL\\17\\data\\session_events.csv'
DELIMITER ','
CSV HEADER;

-- Query first 5 rows of session_events table
SELECT *
FROM session_events
LIMIT 5;


-- Exploring customers table

-- Total customers
SELECT COUNT(*)
FROM customers;

-- Total unique customers
SELECT COUNT(DISTINCT(full_name, age, city, gender, state, yearly_income, education, occupation))
FROM customers;

-- Checking for Duplicate rows
SELECT DISTINCT(full_name, age, city, gender, state, yearly_income, education, occupation),
	COUNT(1) AS frequency
FROM customers
GROUP BY DISTINCT(full_name, age, city, gender, state, yearly_income, education, occupation)
ORDER BY frequency DESC;

-- Removing Duplicate records from customers table
DELETE FROM customers
WHERE customer_id IN 
(
SELECT customer_id
FROM (
	SELECT *,
		ROW_NUMBER() OVER(PARTITION BY full_name, age, city, gender, state, yearly_income, education, occupation) AS rn
	FROM customers
) AS t
WHERE rn > 1
);

-- Checking for missing values
SELECT COUNT(*) - COUNT(full_name) AS full_name_missing,
	 COUNT(*) - COUNT(age) AS age_missing,
	 COUNT(*) - COUNT(city) AS city_missing,
	 COUNT(*) - COUNT(gender) AS gender_missing,
	 COUNT(*) - COUNT(state) AS state_missing,
	 COUNT(*) - COUNT(yearly_income) AS yearly_income_missing,
	 COUNT(*) - COUNT(education) AS education_missing,
	 COUNT(*) - COUNT(occupation) AS occupation_missing
FROM customers;

-- Checking out age column
SELECT MIN(age) AS youngest_customer,
	MAX(age) AS oldest_customer,
	ROUND(AVG(age),2) AS avg_age,
	PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY age) AS median_age
FROM customers;

-- Checking out outliers in age column
WITH quartiles AS (
  SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY age) AS q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY age) AS q3
  FROM customers
)
SELECT *
FROM customers, quartiles
WHERE age > (q3 + 1.5 * (q3 - q1)) -- Upper fence
   OR age < (q1 - 1.5 * (q3 - q1)) -- Lower fence
ORDER BY age DESC;

-- Checking the age column, we have found some rows with customers age as 120. 
-- We will remove all customers above age of 100 because these are extreme outliers.
DELETE FROM customers
WHERE age > 100;

-- Checking out city column
SELECT city,
	COUNT(1) AS number_of_customers
FROM customers
GROUP BY city
ORDER BY number_of_customers DESC;

-- Checking out gender column
SELECT gender,
	COUNT(1) AS number_of_customers
FROM customers
GROUP BY gender
ORDER BY number_of_customers DESC;

-- Checking out state column
SELECT state,
	COUNT(1) AS number_of_customers
FROM customers
GROUP BY state
ORDER BY number_of_customers DESC;

-- Checking out yearly_income column
SELECT MIN(yearly_income) AS youngest_customer,
	MAX(yearly_income) AS oldest_customer,
	ROUND(AVG(yearly_income),2) AS avg_yearly_income,
	PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY yearly_income) AS median_yearly_income
FROM customers;

-- Checking out education column
SELECT education,
	COUNT(1) AS number_of_customers
FROM customers
GROUP BY education
ORDER BY number_of_customers DESC;

-- Checking out occupation column
SELECT occupation,
	COUNT(1) AS number_of_customers
FROM customers
GROUP BY occupation
ORDER BY number_of_customers DESC;

-- checking combination of city and state
SELECT city, 
	state,
	COUNT(1) AS num_of_customers
FROM customers
GROUP BY 1, 2
ORDER BY state;

-- Updating city column based on values from city, state combinations given here
-- Delhi state can only have Delhi has city
-- State of Gujarat only has data for Ahmedabad city
-- State of Karnataka only has data for Bangalore city
-- State of Rajasthan only has data for Jaipur city
-- State of Telangana only has data for Hyderabad city
-- State of Uttar Pradesh only has data for Lucknow city
-- State of Tamil Nadu only has data for Chennai city
-- State of West Bengal only has data for Kolkata city
-- State of Maharashtra has data for two cities Pune and Mumbai, so based on percentage of occurence we are going to fill cities.

-- Replacing null with Delhi
UPDATE customers
SET city = 'Delhi'
WHERE city IS NULL AND state = 'Delhi';

-- Replacing null with Ahmedabad
UPDATE customers
SET city = 'Ahmedabad'
WHERE city IS NULL AND state = 'Gujarat';

-- Replacing null with Bangalore
UPDATE customers
SET city = 'Bangalore'
WHERE city IS NULL AND state = 'Karnataka';

-- Replacing null with Jaipur
UPDATE customers
SET city = 'Jaipur'
WHERE city IS NULL AND state = 'Rajasthan';

-- Replacing null with Hyderabad
UPDATE customers
SET city = 'Hyderabad'
WHERE city IS NULL AND state = 'Telangana';

-- Replacing null with Chennai
UPDATE customers
SET city = 'Chennai'
WHERE city IS NULL AND state = 'Tamil Nadu';

-- Replacing null with Lucknow
UPDATE customers
SET city = 'Lucknow'
WHERE city IS NULL AND state = 'Uttar Pradesh';

-- Replacing null with West Bengal
UPDATE customers
SET city = 'Kolkata'
WHERE city IS NULL AND state = 'West Bengal';

-- Filling Maharashtra values
SELECT state,
	city,
	(COUNT(*) :: NUMERIC)/ (SELECT COUNT(*) FROM customers WHERE state='Maharashtra') * 100
FROM customers
WHERE state = 'Maharashtra'
GROUP BY 1,2;

-- Seeing that Mumbai is having 75% proportion in Maharashtra and rest in Pune, 
-- we will fill those 77 missing values based on this proportion. 
-- so, we will fill first 57 values with Mumbai and 20 with Pune city.

WITH CTE AS (
	SELECT 
		customer_id,
       	city, state,
        ROW_NUMBER() OVER (ORDER BY customer_id) AS rn
    FROM customers
    WHERE city IS NULL AND state = 'Maharashtra'
)

UPDATE customers
SET city = CASE
				WHEN rn <= 57 THEN 'Mumbai'
				ELSE 'Pune'
			END
FROM CTE
WHERE customers.city IS NULL
AND customers.state = 'Maharashtra'
AND customers.customer_id = CTE.customer_id;

-- Checking out state column to fill values
SELECT city, 
	state,
	COUNT(1) AS num_of_customers
FROM customers
GROUP BY 1, 2
ORDER BY state;

-- Based on state data, we would fill state null values from city names.
-- Delhi state can only have Delhi has city,
-- Ahmedabad is in Gujarat,
-- Bangalore is in Karnataka, 
-- Jaipur is in Rajasthan, 
-- Hyderabad is in Telangana, 
-- Lucknow is in Uttar Pradesh, 
-- Chennai is in Tamil Nadu, 
-- Kolkata is in West Bengal
-- Pune and Mumbai are in Maharashtra

-- Replacing null in state of Delhi
UPDATE customers
SET state = 'Delhi'
WHERE state IS NULL
	AND city = 'Delhi';

-- Replacing null in state of Gujarat
UPDATE customers
SET state = 'Gujarat'
WHERE state IS NULL
	AND city = 'Ahmedabad';

-- Replacing null in state of Karnataka
UPDATE customers
SET state = 'Karnataka'
WHERE state IS NULL
	AND city = 'Bangalore';

-- Replacing null in state of Rajasthan
UPDATE customers
SET state = 'Rajasthan'
WHERE state IS NULL
	AND city = 'Jaipur';
	
-- Replacing null in state of Telangana
UPDATE customers
SET state = 'Telangana'
WHERE state IS NULL
	AND city = 'Hyderabad';

-- Replacing null in state of Uttar Pradesh
UPDATE customers
SET state = 'Uttar Pradesh'
WHERE state IS NULL
	AND city = 'Lucknow';

-- Replacing null in state of Tamil Nadu
UPDATE customers
SET state = 'Tamil Nadu'
WHERE state IS NULL
	AND city = 'Chennai';

-- Replacing null in state of West Bengal
UPDATE customers
SET state = 'West Bengal'
WHERE state IS NULL
	AND city = 'Kolkata';

-- Replacing null in state of Maharashtra
UPDATE customers
SET state = 'Maharashtra'
WHERE state IS NULL
	AND (city = 'Pune' OR city = 'Mumbai');

-- Still found that city and state both have 3 same missing values,
-- we will check the combination of education, occupation, and city with count of customers,
-- and fill those values in customer table.
WITH CTE AS(
	SELECT education,
		occupation,
		city
	FROM (
		SELECT education,
			occupation,
			city,
			ROW_NUMBER() OVER(PARTITION BY education, occupation ORDER BY COUNT(1) DESC) AS rank
		FROM customers
		GROUP BY 1,2,3 ) AS t
	WHERE rank = 1
	)

UPDATE customers
SET city = CTE.city
FROM CTE
WHERE customers.education = CTE.education
	AND customers.occupation = CTE.occupation
	AND customers.city IS NULL;

-- Finding which state has missing values to check for distinct city names
SELECT DISTINCT city
FROM customers
WHERE state IS NULL;

-- Filling state missing values for city Bangalore
UPDATE customers
SET state = 'Karnataka'
WHERE state IS NULL
	AND city = 'Bangalore';

-- Filling state missing values for city Ahmedabad
UPDATE customers
SET state = 'Gujarat'
WHERE state IS NULL
	AND city = 'Ahmedabad';

-- Filling null values in age column
-- Checking mean age per gender
SELECT gender,
	COUNT(*),
	AVG(age) 
FROM customers
GROUP BY gender;

-- Checking mean age per city
SELECT city, AVG(age)
FROM customers
GROUP BY city;

-- Age is similar across gender and city, so we will replace age null values with mean of age column.
UPDATE customers
SET age = (SELECT ROUND(AVG(age),0) FROM customers)
WHERE age IS NULL;

-- Replacing the null values in yearly_income column based on mean values per occupation, city group.
-- This is done because the place of work and occupation matters for yearly income.
WITH CTE AS (
SELECT 
	occupation,
	city,
	ROUND(AVG(yearly_income)) AS avg_yearly_income
FROM customers
WHERE occupation IS NOT NULL 
	AND city IS NOT NULL
GROUP BY occupation, city
)

UPDATE customers
SET yearly_income = CTE.avg_yearly_income
FROM CTE
WHERE customers.occupation = CTE.occupation
AND customers.city = CTE.city
AND customers.yearly_income IS NULL;

-- Checking for missing values again in yearly income column
SELECT occupation, city, yearly_income
FROM customers
WHERE yearly_income IS NULL;

-- Upon checking there are still some missing values for yearly_income in customers table
-- We will fill on the basis of mean yearly income for that occupation (if available), 
-- then fill based on city's mean yearly income.
-- Filling null based on occupation first
WITH CTE AS (
SELECT 
	occupation,
	ROUND(AVG(yearly_income)) AS avg_yearly_income
FROM customers
WHERE occupation IS NOT NULL 
GROUP BY occupation
)

UPDATE customers
SET yearly_income = CTE.avg_yearly_income
FROM CTE
WHERE customers.occupation = CTE.occupation
AND customers.yearly_income IS NULL;

-- Fillin null in yearly_income based on city
WITH CTE AS (
SELECT 
	city,
	ROUND(AVG(yearly_income)) AS avg_yearly_income
FROM customers
WHERE city IS NOT NULL
GROUP BY city
)

UPDATE customers
SET yearly_income = CTE.avg_yearly_income
FROM CTE
WHERE customers.city = CTE.city
AND customers.yearly_income IS NULL;

-- For filling missing values in education column, checked for different column wise data.
-- Decided to go through with grouped by values for each city, occupation combination with most likely educational qualifications.
-- For example, for city of Ahmedabad, most Accountants are having a B.Com education, so we will fill those null values in this
-- city, occupation combination with B.Com, and so no.
WITH CTE AS (
	SELECT city, 
		occupation,
		education
	FROM (
		SELECT city,
			occupation,
			education,
			ROW_NUMBER() OVER(PARTITION BY city, occupation ORDER BY COUNT(1) DESC) AS rank
		FROM customers
		WHERE occupation IS NOT NULL
			AND education IS NOT NULL
		GROUP BY 1, 2, 3) AS t
	WHERE rank = 1
)

UPDATE customers
SET education = CTE.education
FROM CTE
WHERE customers.city = CTE.city
	AND customers.occupation = CTE.occupation
	AND customers.education IS NULL;

-- Checking remaining missing values in education column
SELECT *
FROM customers
WHERE education IS NULL;

-- First we will fill null values in occupation then update the education column 
-- Filling null values in occupation based on city wise most occuring occupation for customers
WITH CTE AS (
	SELECT city, 
		occupation
	FROM (
		SELECT city,
			occupation,
			ROW_NUMBER() OVER(PARTITION BY city ORDER BY COUNT(1) DESC) AS rank
		FROM customers
		WHERE occupation IS NOT NULL
		GROUP BY 1, 2) AS t
	WHERE rank = 1
	)
	
UPDATE customers
SET occupation = CTE.occupation
FROM CTE
WHERE customers.city = CTE.city
	AND customers.occupation IS NULL;


-- Running the previous Update query again to fill null values in education where occupation was missing 
WITH CTE AS (
	SELECT city, 
		occupation,
		education
	FROM (
		SELECT city,
			occupation,
			education,
			ROW_NUMBER() OVER(PARTITION BY city, occupation ORDER BY COUNT(1) DESC) AS rank
		FROM customers
		WHERE occupation IS NOT NULL
			AND education IS NOT NULL
		GROUP BY 1, 2, 3) AS t
	WHERE rank = 1
)

UPDATE customers
SET education = CTE.education
FROM CTE
WHERE customers.city = CTE.city
	AND customers.occupation = CTE.occupation
	AND customers.education IS NULL;

-- Checking for missing values
SELECT COUNT(*) - COUNT(full_name) AS full_name_missing,
	 COUNT(*) - COUNT(age) AS age_missing,
	 COUNT(*) - COUNT(city) AS city_missing,
	 COUNT(*) - COUNT(gender) AS gender_missing,
	 COUNT(*) - COUNT(state) AS state_missing,
	 COUNT(*) - COUNT(yearly_income) AS yearly_income_missing,
	 COUNT(*) - COUNT(education) AS education_missing,
	 COUNT(*) - COUNT(occupation) AS occupation_missing
FROM customers;

-- Exploring transactions table

-- Remove all transactions before 2024-03-19
DELETE FROM transactions
WHERE transaction_date < '2024-03-19';

-- Querying first 5 rows
SELECT *
FROM transactions
LIMIT 5;

-- Check for number of records
SELECT COUNT(*)
FROM transactions;

-- Check for duplicate rows
SELECT COUNT(DISTINCT(session_id, transaction_date))
FROM transactions;

-- Checking for missing values
SELECT 
	 COUNT(*) - COUNT(customer_id) AS customer_id_missing,
	 COUNT(*) - COUNT(purchase_amount) AS purchase_amount_missing,
	 COUNT(*) - COUNT(product_category) AS product_category_missing,
	 COUNT(*) - COUNT(payment_method) AS payment_method_missing,
	 COUNT(*) - COUNT(transaction_date) AS transaction_date_missing
FROM transactions;

-- There are no missing values in transactions table

-- Let us start exploring the dataset
-- Exploring customer_id column
SELECT COUNT(DISTINCT(customer_id)) AS customers_that_placed_order
FROM transactions;

-- Exploring purchase_amount column
SELECT MIN(purchase_amount) AS min_purchase_amount,
	AVG(purchase_amount) AS avg_purchase_amount,
	PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY purchase_amount) AS median_purchase_amount,
	MAX(purchase_amount) AS max_purchase_amount
FROM transactions;

-- Detecting outliers in purchase_amount column so will check for that
SELECT q3 + (1.5 * (q3-q1)) as upper_bound,
	GREATEST(q1 - (1.5 * (q3-q1)), 0) as lower_bound
FROM (
	SELECT PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY purchase_amount) as q3,
		PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY purchase_amount) as q1
	FROM transactions) AS t;

-- Seeing the upper and lower bound and median of purchase_amount, we can say that there are no extreme outliers in the column.

-- Exploring purchase_category column
SELECT product_category,
	COUNT(*) AS transaction_count
FROM transactions
GROUP By product_category
ORDER BY transaction_count DESC;

-- There are 9 unique category of products and customers purchased Luxury Fashion category most of the times.
-- Also, they purchased from books category of products the least. 

-- Exploring payment_method column
SELECT payment_method,
	COUNT(*) AS transaction_count
FROM transactions
GROUP By payment_method
ORDER BY transaction_count DESC;

-- Exploring transaction_date column
-- Monthly transactions
SELECT EXTRACT(MONTH FROM transaction_date) AS month,
	COUNT(*) AS num_txns
FROM transactions
GROUP BY month
ORDER BY month;

-- Yearly transactions
SELECT EXTRACT(year FROM transaction_date) AS year,
	COUNT(*) AS num_txns
FROM transactions
GROUP BY year
ORDER BY year;

-- transactions day wise
SELECT TO_CHAR(transaction_date, 'Day') AS day_of_week,
	COUNT(*) AS num_txns
FROM transactions
GROUP BY day_of_week;

-- Exploring churn table
-- Query first 5 rows
SELECT *
FROM churn
LIMIT 5;

-- Counting records
SELECT COUNT(*)
FROM churn;

-- total orders column
SELECT SUM(total_orders) AS sum_of_total_orders
FROM churn;

-- total orders stats
SELECT MIN(total_orders) AS minimum_total_orders,
	ROUND(AVG(total_orders),2) AS avg_total_orders,
	PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY total_orders) AS median_total_orders,
	MAX(total_orders) AS maximum_total_orders
FROM churn;

-- days_since_last_purchase column
SELECT MIN(days_since_last_purchase) AS minimum_days_since_last_purchase,
	ROUND(AVG(days_since_last_purchase),2) AS avg_days_since_last_purchase,
	PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY days_since_last_purchase) AS median_days_since_last_purchase,
	MAX(days_since_last_purchase) AS maximum_days_since_last_purchase
FROM churn;

-- Checking for duplicates
SELECT COUNT(DISTINCT(customer_id, total_orders, days_since_last_purchase, last_purchase_date))
FROM churn;

-- Checking for missing values
SELECT 
	 COUNT(*) - COUNT(customer_id) AS customer_id_missing,
	 COUNT(*) - COUNT(total_orders) AS total_orders_missing,
	 COUNT(*) - COUNT(days_since_last_purchase) AS days_since_last_purchase_missing,
	 COUNT(*) - COUNT(last_purchase_date) AS last_purchase_date_missing
FROM churn;


-- SESSION_EVENT TABLE

-- Remove all session events before 2024-03-19
DELETE FROM session_events
WHERE timestamp < '2024-03-19';

-- Query first 5 rows
SELECT *
FROM session_events
LIMIT 5;

-- Found Duplicate rows
SELECT COUNT(*) - COUNT(DISTINCT(customer_id, event_type, timestamp)) AS duplicates
FROM session_events
WHERE timestamp IS NOT NULL;

-- Found 3 duplicate rows,
-- Solving this issue by removing exact duplicates

-- Checking rows that are duplicated without null values in timestamp
WITH dup AS (
  SELECT
    session_id,
    customer_id,
    event_type,
    timestamp,
    COUNT(*) OVER (PARTITION BY session_id) AS event_count,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id, event_type, timestamp
      ORDER BY ctid
    ) AS rn
  FROM session_events
  WHERE timestamp IS NOT NULL
)
SELECT
  session_id,
  customer_id,
  event_type,
  timestamp,
  event_count
FROM dup
WHERE rn > 1
ORDER BY customer_id, session_id;

-- Deleting the duplicated rows
WITH to_delete AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id, event_type, timestamp
      ORDER BY ctid
    ) AS rn
  FROM session_events
  WHERE timestamp IS NOT NULL
)
DELETE FROM session_events
WHERE ctid IN (
  SELECT ctid
  FROM to_delete
  WHERE rn > 1
);

-- Checking for Sessions with more than one unique events.

-- Finding those sessions and deleting the duplicate event.
WITH duplicate_events AS (
SELECT ctid,
	ROW_NUMBER() OVER(PARTITION BY session_id, customer_id, event_type ORDER BY timestamp DESC) AS rn
FROM session_events 
WHERE session_id IN (
	SELECT session_id
	FROM session_events
	GROUP BY session_id, customer_id, event_type
	HAVING COUNT(*) > 1)
)

-- Deleting those rows
DELETE FROM session_events
WHERE ctid IN (
	SELECT ctid 
	FROM duplicate_events
	WHERE rn > 1);

-- Deleted in total 2082 such rows.

-- Checking for Sessions with more than one customer_id

-- Upon Checking, found no such rows.
SELECT session_id,
  COUNT(DISTINCT customer_id) AS distinct_customer_count
FROM session_events
GROUP BY session_id
HAVING COUNT(DISTINCT customer_id) > 1
ORDER BY distinct_customer_count DESC;

-- All duplicates have been dealt with.

-- Checking for Sessions with all events timestamp missing
SELECT
  session_id,
  COUNT(*) AS event_count,
  COUNT(timestamp) AS non_missing_events
FROM session_events
GROUP BY session_id
HAVING COUNT(timestamp) = 0
ORDER BY session_id;

-- There are 1164 such sessions and 1170 such records to be removed.
DELETE FROM session_events
WHERE session_id IN (
	SELECT session_id
	FROM session_events
	GROUP BY session_id
	HAVING COUNT(timestamp) = 0);
	
-- Checking remaining missing timestamps
SELECT COUNT(*) - COUNT(timestamp) AS timestamp_missing
FROM session_events;

-- Checking for Missing events in a session

-- Checking for missing events
WITH skipped_events as (
	SELECT session_id,
		COUNT(DISTINCT event_type) AS count_of_events,
		MAX(event_organize) AS max_event
	FROM (
		SELECT *,
			CASE
				WHEN event_type = 'visit' THEN 1
				WHEN event_type = 'add_to_cart' THEN 2
				WHEN event_type = 'checkout' THEN 3
				ELSE 4
			END AS event_organize
		FROM session_events) AS t
	GROUP BY session_id)

SELECT *
FROM skipped_events
WHERE count_of_events != max_event;

-- There are 4968 such records to be cleaned

-- We will use a Stored Procedure to fill any missing events,
-- but would add null timestamps to deal later.
-- Optimized Query for Stored Procedure
CREATE OR REPLACE PROCEDURE fix_missing_events()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create a temporary table with session summaries for faster processing
    CREATE TEMP TABLE session_summary AS
    SELECT 
        session_id, 
        customer_id,
        COUNT(DISTINCT event_type) AS event_count,
        BOOL_OR(event_type = 'visit') AS has_visit,
        BOOL_OR(event_type = 'add_to_cart') AS has_add_to_cart,
        BOOL_OR(event_type = 'checkout') AS has_checkout,
        BOOL_OR(event_type = 'purchase') AS has_purchase
    FROM session_events
    GROUP BY session_id, customer_id;
    
    -- Add all missing events in bulk operations (much faster than row-by-row)
    
    -- Add missing visit events
    INSERT INTO session_events (session_id, customer_id, event_type, timestamp)
    SELECT 
        session_id, 
        customer_id, 
        'visit', 
        NULL
    FROM session_summary
    WHERE NOT has_visit AND (has_add_to_cart OR has_checkout OR has_purchase);
    
    -- Add missing add_to_cart events
    INSERT INTO session_events (session_id, customer_id, event_type, timestamp)
    SELECT 
        session_id, 
        customer_id, 
        'add_to_cart', 
        NULL
    FROM session_summary
    WHERE NOT has_add_to_cart AND (has_checkout OR has_purchase);
    
    -- Add missing checkout events
    INSERT INTO session_events (session_id, customer_id, event_type, timestamp)
    SELECT 
        session_id, 
        customer_id, 
        'checkout', 
        NULL
    FROM session_summary
    WHERE NOT has_checkout AND has_purchase;
    
    -- Clean up
    DROP TABLE session_summary;
END;
$$;

-- Calling the Stored Procedure
CALL fix_missing_events();

-- The missing timestamps have increased to 9265 values because we added missing events and added null timestamps.

-- Checking for Wrong order of Events
SELECT
  se.*,
  ROW_NUMBER() OVER (
    PARTITION BY session_id
    ORDER BY timestamp ASC NULLS LAST
  ) AS actual_order,
  CASE se.event_type
    WHEN 'visit'       THEN 1
    WHEN 'add_to_cart' THEN 2
    WHEN 'checkout'    THEN 3
    WHEN 'purchase'    THEN 4
  END AS correct_order
FROM session_events AS se;

-- Fixing the wrong order of events
BEGIN;

WITH ordered AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY session_id
      ORDER BY timestamp ASC NULLS LAST
    ) AS rn
  FROM session_events
)
UPDATE session_events se
SET event_type = CASE ordered.rn
  WHEN 1 THEN 'visit'
  WHEN 2 THEN 'add_to_cart'
  WHEN 3 THEN 'checkout'
  WHEN 4 THEN 'purchase'
  ELSE se.event_type
END
FROM ordered
WHERE se.ctid = ordered.ctid;

--Rollback;
Commit;

-- Checking remaining missing values
SELECT COUNT(*) - COUNT(timestamp) AS timestamp_missing
FROM session_events;

-- Fixing for Null values

-- Steps we will follow are:
	-- Finding the customer event averages in time, then filling the missing values in timestamp
	-- Finding global event averages, then filling remaining missing values in timestamp 

-- Finding each customer average time between events
DROP TABLE IF EXISTS customer_event_avg;
CREATE TEMP TABLE customer_event_avg AS
WITH session_times AS (
  SELECT
    session_id,
    customer_id,
    MAX(CASE WHEN event_type = 'visit'       THEN timestamp END) AS visit_ts,
    MAX(CASE WHEN event_type = 'add_to_cart' THEN timestamp END) AS add_ts,
    MAX(CASE WHEN event_type = 'checkout'    THEN timestamp END) AS checkout_ts,
    MAX(CASE WHEN event_type = 'purchase'    THEN timestamp END) AS purchase_ts
  FROM session_events
  GROUP BY session_id, customer_id
)
SELECT
  customer_id,
  AVG(EXTRACT(EPOCH FROM (add_ts      - visit_ts  )))   AS avg_v2a_secs,
  AVG(EXTRACT(EPOCH FROM (checkout_ts - add_ts    )))   AS avg_a2c_secs,
  AVG(EXTRACT(EPOCH FROM (purchase_ts - checkout_ts))) AS avg_c2p_secs
FROM session_times
WHERE
  (visit_ts    IS NOT NULL AND add_ts      IS NOT NULL)
  OR (add_ts     IS NOT NULL AND checkout_ts IS NOT NULL)
  OR (checkout_ts IS NOT NULL AND purchase_ts IS NOT NULL)
GROUP BY customer_id;

-- Checking the table created
SELECT *
FROM customer_event_avg
LIMIT 5;

-- Prepare a temp view of each session’s four event timestamps
DROP VIEW IF EXISTS session_times;
CREATE TEMP VIEW session_times AS
SELECT
  session_id,
  customer_id,
  MAX(CASE WHEN event_type = 'visit'       THEN timestamp END) AS visit_ts,
  MAX(CASE WHEN event_type = 'add_to_cart' THEN timestamp END) AS add_ts,
  MAX(CASE WHEN event_type = 'checkout'    THEN timestamp END) AS checkout_ts,
  MAX(CASE WHEN event_type = 'purchase'    THEN timestamp END) AS purchase_ts
FROM session_events
GROUP BY session_id, customer_id;

-- 1) Backfill VISIT from ADD_TO_CART
UPDATE session_events se
SET timestamp = date_trunc(
  'second',
  st.add_ts - INTERVAL '1 second' * cea.avg_v2a_secs
)
FROM session_times st
JOIN customer_event_avg cea ON cea.customer_id = st.customer_id
WHERE se.session_id   = st.session_id
  AND se.customer_id  = st.customer_id
  AND se.event_type   = 'visit'
  AND se.timestamp   IS NULL
  AND st.add_ts      IS NOT NULL
  AND cea.avg_v2a_secs IS NOT NULL
;

-- 2) Fill missing ADD_TO_CART when VISIT exists
UPDATE session_events se
SET timestamp = date_trunc(
  'second',
  st.visit_ts + INTERVAL '1 second' * cea.avg_v2a_secs
)
FROM session_times st
JOIN customer_event_avg cea ON cea.customer_id = st.customer_id
WHERE se.session_id   = st.session_id
  AND se.customer_id  = st.customer_id
  AND se.event_type   = 'add_to_cart'
  AND se.timestamp   IS NULL
  AND st.visit_ts    IS NOT NULL
  AND cea.avg_v2a_secs IS NOT NULL
;

-- 3) Backfill ADD_TO_CART from CHECKOUT
UPDATE session_events se
SET timestamp = date_trunc(
  'second',
  st.checkout_ts - INTERVAL '1 second' * cea.avg_a2c_secs
)
FROM session_times st
JOIN customer_event_avg cea ON cea.customer_id = st.customer_id
WHERE se.session_id     = st.session_id
  AND se.customer_id    = st.customer_id
  AND se.event_type     = 'add_to_cart'
  AND se.timestamp     IS NULL
  AND st.checkout_ts   IS NOT NULL
  AND cea.avg_a2c_secs IS NOT NULL
;

-- 4) Fill missing CHECKOUT when ADD_TO_CART exists
UPDATE session_events se
SET timestamp = date_trunc(
  'second',
  st.add_ts + INTERVAL '1 second' * cea.avg_a2c_secs
)
FROM session_times st
JOIN customer_event_avg cea ON cea.customer_id = st.customer_id
WHERE se.session_id     = st.session_id
  AND se.customer_id    = st.customer_id
  AND se.event_type     = 'checkout'
  AND se.timestamp     IS NULL
  AND st.add_ts        IS NOT NULL
  AND cea.avg_a2c_secs IS NOT NULL
;

-- 5) Backfill CHECKOUT from PURCHASE
UPDATE session_events se
SET timestamp = date_trunc(
  'second',
  st.purchase_ts - INTERVAL '1 second' * cea.avg_c2p_secs
)
FROM session_times st
JOIN customer_event_avg cea ON cea.customer_id = st.customer_id
WHERE se.session_id      = st.session_id
  AND se.customer_id     = st.customer_id
  AND se.event_type      = 'checkout'
  AND se.timestamp      IS NULL
  AND st.purchase_ts    IS NOT NULL
  AND cea.avg_c2p_secs IS NOT NULL
;

-- 6) Forward-fill PURCHASE from CHECKOUT
UPDATE session_events se
SET timestamp = date_trunc(
  'second',
  st.checkout_ts + INTERVAL '1 second' * cea.avg_c2p_secs
)
FROM session_times st
JOIN customer_event_avg cea ON cea.customer_id = st.customer_id
WHERE se.session_id      = st.session_id
  AND se.customer_id     = st.customer_id
  AND se.event_type      = 'purchase'
  AND se.timestamp      IS NULL
  AND st.checkout_ts    IS NOT NULL
  AND cea.avg_c2p_secs IS NOT NULL
;

-- Dropping the VIEW
DROP VIEW IF EXISTS session_times;


-- Checking remaining missing values
SELECT COUNT(*) - COUNT(timestamp) AS timestamp_missing
FROM session_events;

-- Fixing Remaining Missing Values

-- Calculating global average time between events across all customers
DROP TABLE IF EXISTS global_event_avg;
CREATE TEMP TABLE global_event_avg AS
WITH session_times AS (
  SELECT
    session_id,
    MAX(CASE WHEN event_type = 'visit'       THEN timestamp END) AS visit_ts,
    MAX(CASE WHEN event_type = 'add_to_cart' THEN timestamp END) AS add_ts,
    MAX(CASE WHEN event_type = 'checkout'    THEN timestamp END) AS checkout_ts,
    MAX(CASE WHEN event_type = 'purchase'    THEN timestamp END) AS purchase_ts
  FROM session_events
  GROUP BY session_id
)
SELECT
  AVG(EXTRACT(EPOCH FROM (add_ts       - visit_ts  ))) FILTER (WHERE visit_ts    IS NOT NULL AND add_ts      IS NOT NULL)   AS avg_v2a_secs,
  AVG(EXTRACT(EPOCH FROM (checkout_ts  - add_ts    ))) FILTER (WHERE add_ts      IS NOT NULL AND checkout_ts IS NOT NULL) AS avg_a2c_secs,
  AVG(EXTRACT(EPOCH FROM (purchase_ts  - checkout_ts))) FILTER (WHERE checkout_ts IS NOT NULL AND purchase_ts IS NOT NULL) AS avg_c2p_secs
FROM session_times;

-- Exploring created table: global_event_avg
SELECT *
FROM global_event_avg
LIMIT 5;

--  Back-/forward-fill all remaining NULL timestamps using the global averages
WITH session_times AS (
  SELECT
    session_id,
    customer_id,
    MAX(CASE WHEN event_type = 'visit'       THEN timestamp END) AS visit_ts,
    MAX(CASE WHEN event_type = 'add_to_cart' THEN timestamp END) AS add_ts,
    MAX(CASE WHEN event_type = 'checkout'    THEN timestamp END) AS checkout_ts,
    MAX(CASE WHEN event_type = 'purchase'    THEN timestamp END) AS purchase_ts
  FROM session_events
  GROUP BY session_id, customer_id
)

UPDATE session_events se
SET timestamp = date_trunc(
  'second',
  CASE se.event_type
    WHEN 'visit'       THEN
      COALESCE(
        st.add_ts     - (g.avg_v2a_secs * INTERVAL '1 second')
      )
    WHEN 'add_to_cart' THEN
      COALESCE(
        st.visit_ts   + (g.avg_v2a_secs * INTERVAL '1 second'),
        st.checkout_ts - (g.avg_a2c_secs * INTERVAL '1 second')
      )
    WHEN 'checkout'    THEN
      COALESCE(
        st.add_ts      + (g.avg_a2c_secs * INTERVAL '1 second'),
        st.purchase_ts - (g.avg_c2p_secs * INTERVAL '1 second')
      )
    WHEN 'purchase'    THEN
      COALESCE(
        st.checkout_ts + (g.avg_c2p_secs * INTERVAL '1 second'),
        st.add_ts      + ((g.avg_a2c_secs + g.avg_c2p_secs) * INTERVAL '1 second'),
        st.visit_ts    + ((g.avg_v2a_secs + g.avg_a2c_secs + g.avg_c2p_secs) * INTERVAL '1 second')
      )
    ELSE se.timestamp
  END
)
FROM session_times st
CROSS JOIN global_event_avg g
WHERE se.session_id  = st.session_id
  AND se.customer_id = st.customer_id
  AND se.timestamp   IS NULL;

-- All null values have been filled

-- Since some events and dates have been corrected, we will change the dates in transactions table,
-- then update the churn table based on transactions table.

-- Checking mismatch rows in transactions and session_events
SELECT 
  COUNT(*) AS mismatched_transactions
FROM transactions t
JOIN session_events se
  ON t.session_id  = se.session_id
 AND t.customer_id = se.customer_id
 AND se.event_type = 'purchase'
WHERE t.transaction_date IS DISTINCT FROM se.timestamp;

-- Updating transactions table
UPDATE transactions t
SET transaction_date = se.timestamp
FROM session_events se
WHERE se.event_type = 'purchase'
  AND t.session_id = se.session_id
  AND t.customer_id = se.customer_id
  AND t.transaction_date IS DISTINCT FROM se.timestamp;

-- Since we updated some values we will be checking for duplicates in the transactions table
-- Check for duplicate rows
SELECT COUNT(DISTINCT(session_id, transaction_date))
FROM transactions;

-- We see some duplicates, we will remove them
DELETE FROM transactions
WHERE transaction_id IN (
	SELECT transaction_id
	FROM (
		SELECT transaction_id,
			ROW_NUMBER() OVER(PARTITION BY session_id, transaction_date ORDER BY transaction_id) AS rank
		FROM transactions
		)
	WHERE rank > 1
);

-- Check for duplicate rows after deletion
SELECT COUNT(*)
FROM transactions;

-- Updated the transactions table

-- Checking mismatch rows in transactions and churn
SELECT 
  COUNT(*) AS affected_churn_rows
FROM churn c
JOIN (
  SELECT 
    customer_id,
    MAX(transaction_date) AS new_last_purchase_date
  FROM transactions
  GROUP BY customer_id
) t
  ON c.customer_id = t.customer_id
WHERE c.last_purchase_date IS DISTINCT FROM t.new_last_purchase_date;

-- Updating churn table
UPDATE churn c
SET last_purchase_date = t.new_last_purchase_date
FROM (
  SELECT
    customer_id,
    MAX(transaction_date) AS new_last_purchase_date
  FROM transactions
  GROUP BY customer_id
) AS t
WHERE c.customer_id = t.customer_id
  AND c.last_purchase_date IS DISTINCT FROM t.new_last_purchase_date;
