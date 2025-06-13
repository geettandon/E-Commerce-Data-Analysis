-- E-Commerce End to End Project
-- First leg of the project of data manipulation and cleaning will be in PostgreSQL

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

-- Query customers table
SELECT *
FROM customers
LIMIT 5;

-- Copy the data from csv file to customers table
COPY customers(customer_id, full_name, age, city, gender, state, yearly_income, education, occupation)
FROM 'C:\\Program Files\\PostgreSQL\\17\\data\\customers.csv'
DELIMITER ','
CSV HEADER;

-- Creating transactions table
DROP TABLE IF EXISTS transactions;
Create Table transactions 
(
	transaction_id		INT PRIMARY KEY,		
	customer_id			INT,
	purchase_amount		DECIMAL(10, 2),
	product_category	VARCHAR(50),
	payment_method		VARCHAR(50),
	transaction_date	TIMESTAMP 
);

-- Adding data to transactions table from csv file
COPY transactions(transaction_id, customer_id, purchase_amount, product_category, payment_method,	transaction_date)
FROM 'C:\\Program Files\\PostgreSQL\\17\\data\\transactions_mod_12may.csv'
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
FROM 'C:\\Program Files\\PostgreSQL\\17\\data\\se_new_18may.csv'
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
	 COUNT(*) - COUNT(yearly_income) AS age_missing,
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

-- Still found that city and state both have 3 same missing rows,
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

-- For filling missing values in education column, we checked for different column wise data.
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

-- Querying first 5 rows
SELECT *
FROM transactions
LIMIT 5;

-- Check for number of records
SELECT COUNT(*)
FROM transactions;

-- Check for duplicate rows
SELECT COUNT(DISTINCT(customer_id, purchase_amount, product_category, payment_method, transaction_date))
FROM transactions;

-- We see some duplicates, we will remove them first
DELETE FROM transactions
WHERE transaction_id IN (
	SELECT transaction_id
	FROM (
		SELECT transaction_id,
			ROW_NUMBER() OVER(PARTITION BY customer_id, purchase_amount, product_category, payment_method, transaction_date ORDER BY transaction_id) AS rank
		FROM transactions
		)
	WHERE rank > 1
);

-- Check for duplicate rows after deletion
SELECT COUNT(*)
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
	AVG(total_orders) AS avg_total_orders,
	PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY total_orders) AS median_total_orders,
	MAX(total_orders) AS maximum_total_orders
FROM churn;

-- days_since_last_purchase column
SELECT MIN(days_since_last_purchase) AS minimum_days_since_last_purchase,
	AVG(days_since_last_purchase) AS avg_days_since_last_purchase,
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

-- Exploring session_event
SELECT *
FROM session_events
LIMIT 5;

-- Total rows in table session_events
SELECT COUNT(*)
FROM session_events;

-- Unique sessions
SELECT COUNT(DISTINCT(session_id)) AS sessions
FROM session_events;

-- There are 120000 unique sessions.

-- Checking unique events
SELECT DISTINCT(event_type)
FROM session_events;

-- Exploring duplicates
SELECT COUNT(DISTINCT(customer_id, event_type, timestamp)) AS unique_records
FROM session_events;

-- There are some duplicates in the table.

-- Checking out sessions with more than one customer_ids
SELECT *
FROM session_events
WHERE session_id IN (
SELECT session_id
FROM session_events
GROUP BY 1
HAVING COUNT(DISTINCT (customer_id)) > 1)
ORDER BY session_id;

-- We will now update the wrong customer_id with correct customer_id in some sessions
-- Step 1: Find the most frequent customer_id per session_id
WITH most_frequent_customer AS (
    SELECT session_id, 
		customer_id,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY COUNT(*) DESC) as rn
    FROM session_events
	WHERE session_id IN (SELECT session_id
						FROM session_events
						GROUP BY 1
						HAVING COUNT(DISTINCT (customer_id)) > 1)
    GROUP BY session_id, customer_id
),
correct_customer AS (
    SELECT session_id, 
		customer_id as correct_customer_id
    FROM most_frequent_customer
    WHERE rn = 1
)
-- Step 2: Update mismatched rows
UPDATE session_events
SET customer_id = correct_customer.correct_customer_id
FROM correct_customer
WHERE session_events.session_id = correct_customer.session_id
  AND session_events.customer_id <> correct_customer.correct_customer_id;

-- Deleting the duplicate events that occured in same session_id because one customer can have only one event in one session.
-- based on condition of removing those duplicate records that appeared at later time than first duplicate event.
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

DELETE FROM session_events
WHERE ctid IN (
	SELECT ctid 
	FROM duplicate_events
	WHERE rn > 1);

-- Checking the duplicates in combination of customer_id, event_type, timestamp columns that is not possible to happen
-- as same customer cannot have same events in different sessions.
-- We will remove those rows based on the session_id with least events where that duplicate is present and we shall not remove 
-- the actual session_id event.

-- Checking the result out
WITH cte AS (
	SELECT *
	FROM (
		SELECT *,ctid,
			ROW_NUMBER() OVER(PARTITION BY customer_id, event_type, timestamp) as rn
		FROM session_events
		WHERE timestamp IS NOT NULL) AS t
	WHERE rn > 1	
),
events AS (
	SELECT *,
		COUNT(event_type) OVER(PARTITION BY session_id) AS event_count
	FROM session_events
	)
SELECT
	events.session_id,
	cte.customer_id,
	cte.event_type,
	cte.timestamp,
	events.event_count
FROM events
JOIN cte
ON events.customer_id = cte.customer_id
	AND events.event_type = cte.event_type
	AND events.timestamp = cte.timestamp
ORDER BY customer_id, session_id;

-- Removing the duplicate rows using Delete command
WITH cte AS (
	SELECT *
	FROM (
		SELECT *,ctid,
			ROW_NUMBER() OVER(PARTITION BY customer_id, event_type, timestamp) as rn
		FROM session_events
		WHERE timestamp IS NOT NULL) AS t
	WHERE rn > 1	
),
events AS (
	SELECT *,
		COUNT(event_type) OVER(PARTITION BY session_id) AS event_count
	FROM session_events
	),
	
session_with_count AS (
	SELECT
		events.session_id,
		cte.customer_id,
		cte.event_type,
		cte.timestamp,
		events.event_count,
		ROW_NUMBER() OVER(PARTITION BY cte.customer_id, cte.event_type, cte.timestamp ORDER BY events.event_count ASC) AS rn
	FROM events
	JOIN cte
	ON events.customer_id = cte.customer_id
		AND events.event_type = cte.event_type
		AND events.timestamp = cte.timestamp
	ORDER BY customer_id, session_id
	)

-- Deleting the duplicates
DELETE FROM session_events
WHERE (session_id, customer_id, event_type, timestamp) IN (
SELECT 
	session_id,
	customer_id,
	event_type,
	timestamp
FROM session_with_count
WHERE rn = 1);

-- All duplicates have been dealt with in the table.

-- Now we will check out missing values in the table.
-- Checking for missing values
SELECT 
	 COUNT(*) - COUNT(session_id) AS session_id_missing,
	 COUNT(*) - COUNT(customer_id) AS customer_id_missing,
	 COUNT(*) - COUNT(event_type) AS event_type_missing,
	 COUNT(*) - COUNT(timestamp) AS timestamp_missing
FROM session_events;

-- There are 4897 missing values in timestamp column.
-- After checking out missing values in timestamp, some session have all events with missing timestamp. 
-- We will only remove rows where full session events have no timestamps.

-- Finding count of unique session_id where all timestamps are missing
SELECT COUNT(*) AS number_of_rows
FROM session_events
WHERE session_id IN (
	SELECT session_id
	FROM session_events
	GROUP BY session_id
	HAVING COUNT(timestamp) = 0);

-- There are 1168 sessions and 1174 records where all timestamps are missing.
-- First we will remove all those session_id with all timestamps missing
DELETE FROM session_events
WHERE session_id IN (
	SELECT session_id
	FROM session_events
	GROUP BY session_id
	HAVING COUNT(timestamp) = 0);
	
-- Checking remaining missing timestamps
SELECT COUNT(*) - COUNT(timestamp) AS timestamp_missing
FROM session_events;

-- There are still more than 3700 missing timestamps.
-- Before dealing with these missing timestamps, found that some sessions have wrong order of events.
-- Checking sessions with skipped events
WITH skipped_events as (
	SELECT session_id,
		COUNT(event_type) AS count_of_events,
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

-- Examples to checkout the skipped events.
SELECT *
FROM session_events
WHERE session_id IN (11202, 22453, 24108);

-- Now, we will add missing events in sessions which will introduce more nulls, than we will deal with all null timestamps together.

-- Creating Stored Procedure to find any skipped events between events and filling them with null values.
-- Skipped events appear in sessions like customer went from visit to checkout with add_to_cart event.
-- Because it is a error, we will fix them.
-- Scenarios: 
	-- Does not have visit but has one or all of other events,
	-- Does not have add_to_cart but has checkout and/or purchase,
	-- Does not have checkout but has purchase
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
        COUNT(*) AS event_count,
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

-- Examples to checkout the skipped events to check if stored procedure is working fine or not.
SELECT *
FROM session_events
WHERE session_id IN (11202, 22453, 24108)
ORDER BY session_id;

-- Successfully added the skipped events. 
-- Found another issue of wrong order of events. 
-- So before filling the nulls based on averages we will correct the order of events.

-- Example of sessions where events are present in wrong order.
SELECT *
FROM session_events
WHERE session_id IN (1, 17, 185, 206)
ORDER BY session_id, timestamp;

-- Found that some events comes at the wrong time like add_to_cart event coming after purchase event.
-- Checking out those sessions but filtering those sessions where any event is null.
WITH ranked_events AS (
  SELECT
    *,
    MAX(CASE WHEN event_type = 'purchase' THEN timestamp END) OVER (PARTITION BY session_id) AS purchase_time,
	MAX(CASE WHEN event_type = 'checkout' THEN timestamp END) OVER (PARTITION BY session_id) AS checkout_time,
	MAX(CASE WHEN event_type = 'add_to_cart' THEN timestamp END) OVER (PARTITION BY session_id) AS add_to_cart_time,
	MAX(CASE WHEN event_type = 'visit' THEN timestamp END) OVER (PARTITION BY session_id) AS visit_time
  FROM session_events
  WHERE session_id NOT IN (SELECT session_id FROM session_events WHERE timestamp IS NULL)
)
SELECT *
FROM ranked_events
WHERE
  (event_type = 'visit' AND (timestamp > purchase_time OR timestamp > checkout_time OR timestamp > add_to_cart_time))
  OR 
  (event_type = 'add_to_cart' AND (timestamp > purchase_time OR timestamp > checkout_time))
  OR
  (event_type = 'checkout' AND (timestamp > purchase_time))
ORDER BY session_id, customer_id, timestamp;

-- Creating a Temp Table to save the session_id of these events.
-- For example: purchase event shows timing of add_to_cart event and vice-versa.
DROP TABLE IF EXISTS wrong_order_sessions;
CREATE TEMP TABLE wrong_order_sessions AS
SELECT DISTINCT(session_id) AS session_id
FROM(
	SELECT *,
		CASE
			WHEN event_type = 'visit' THEN 1
			WHEN event_type = 'add_to_cart' THEN 2
			WHEN event_type = 'checkout' THEN 3
			WHEN event_type = 'purchase' THEN 4
		END AS event_order,
		ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY timestamp) AS time_organize  
	FROM session_events
	WHERE session_id NOT IN (SELECT session_id FROM session_events WHERE timestamp IS NULL)
	)
WHERE event_order != time_organize;

-- We found that more than 15000 sessions has misorganized events, so we will correct that.
BEGIN;

WITH wrong_order AS (
    SELECT 
        session_id,
        timestamp,
        event_type AS original_event_type,  -- Store original event type
        ROW_NUMBER() OVER(
            PARTITION BY session_id 
            ORDER BY timestamp
        ) as event_order
    FROM session_events
    WHERE session_id IN (SELECT session_id FROM wrong_order_sessions)
)
UPDATE session_events AS se
SET event_type = CASE 
                    WHEN wo.event_order = 1 THEN 'visit'
                    WHEN wo.event_order = 2 THEN 'add_to_cart'
                    WHEN wo.event_order = 3 THEN 'checkout'
                    WHEN wo.event_order = 4 THEN 'purchase'
                END
FROM wrong_order AS wo
WHERE se.session_id = wo.session_id
    AND se.timestamp = wo.timestamp
    AND se.event_type = wo.original_event_type;  

--ROLLBACK;
COMMIT;

-- Event order is corrected now.
-- Example of session where events were present in wrong order.
SELECT *
FROM session_events
WHERE session_id IN (1, 17, 185, 206)
ORDER BY session_id, timestamp;

-- Checking for missing values before dealing with them.
SELECT COUNT(*) - COUNT(timestamp) AS timestamp_missing
FROM session_events;

-- Now the missing values in timestamp has increased to 8025 because of adding of skipped events.

-- We will start with filling the missing values with average time between events taken by that customer.

-- Getting minute difference between each event and creating a Temp Table
DROP TABLE IF EXISTS customer_avg_event_time;
CREATE TEMP TABLE customer_avg_event_time AS 
WITH event_min_diff AS (
	SELECT *,
		COALESCE(ROUND(EXTRACT(EPOCH from diff) / 60),0) AS minute_diff
	FROM (
		SELECT *,
			timestamp - LAG(timestamp, 1) OVER(PARTITION BY session_id ORDER BY timestamp) AS diff
		FROM session_events
		WHERE session_id NOT IN (SELECT session_id FROM session_events WHERE timestamp IS NULL)
		)
),
-- Creating different columns for each event time difference
event_time_separate AS (
	SELECT *,
		MAX(CASE WHEN event_type = 'visit' THEN minute_diff END) OVER (PARTITION BY session_id) AS visit_time_diff,
		MAX(CASE WHEN event_type = 'add_to_cart' THEN minute_diff END) OVER (PARTITION BY session_id) AS addtocart_time_diff,
		MAX(CASE WHEN event_type = 'checkout' THEN minute_diff END) OVER (PARTITION BY session_id) AS checkout_time_diff,
		MAX(CASE WHEN event_type = 'purchase' THEN minute_diff END) OVER (PARTITION BY session_id) AS purchase_time_diff
	FROM event_min_diff
),
-- Grouping to get one row per session, customer group
session_customer_grouped AS (SELECT session_id, 
	customer_id,
	0 AS visit_time_diff,
	MAX(addtocart_time_diff) AS addtocart_time_diff,
	MAX(checkout_time_diff) AS checkout_time_diff,
	MAX(purchase_time_diff) AS purchase_time_diff
FROM event_time_separate
GROUP BY 1, 2
)

-- Geting round of average difference between each event for each customer
SELECT customer_id,
	ROUND(AVG(addtocart_time_diff), 0) AS avg_time_bw_visit_and_addtocart,
	ROUND(AVG(checkout_time_diff), 0) AS avg_time_bw_addtocart_and_checkout,
	ROUND(AVG(purchase_time_diff), 0) AS avg_time_bw_checkout_and_purchase
FROM session_customer_grouped
GROUP BY 1
ORDER BY customer_id;


-- Querying the created Temp Table
SELECT*
FROM customer_avg_event_time
LIMIT 10;

-- We have created average time between table above. 
-- We will now update the timestamps based on the first available event for every customer.
-- For example: 
-- If customer_id 1 has average time between visit and add_to_cart as 5 minutes, 
-- the query will update the missing timestamp in visit or add_to_cart using either visit or add_to_cart time 
-- adding or subtracting 5 minutes respectively.

BEGIN;

-- Step 1: Create temp table with available timestamps for each session
CREATE TEMP TABLE valid_timestamps AS
SELECT 
    session_id,
    MIN(CASE WHEN event_type = 'visit' THEN timestamp END) AS visit_time,
    MIN(CASE WHEN event_type = 'add_to_cart' THEN timestamp END) AS add_to_cart_time,
    MIN(CASE WHEN event_type = 'checkout' THEN timestamp END) AS checkout_time,
    MIN(CASE WHEN event_type = 'purchase' THEN timestamp END) AS purchase_time,
    customer_id
FROM session_events
WHERE session_id IN (SELECT session_id FROM session_events WHERE timestamp IS NULL)
GROUP BY session_id, customer_id;

-- Step 2: Create an index on the temporary table for faster joins
CREATE INDEX idx_valid_timestamps ON valid_timestamps(session_id, customer_id);

-- Step 3: Update each event type with targeted updates
-- Update visit timestamps
UPDATE session_events se
SET timestamp = 
    CASE 
        WHEN vt.add_to_cart_time IS NOT NULL THEN 
            vt.add_to_cart_time - (ca.avg_time_bw_visit_and_addtocart * INTERVAL '1 minute')
        WHEN vt.checkout_time IS NOT NULL THEN 
            vt.checkout_time - ((ca.avg_time_bw_visit_and_addtocart + ca.avg_time_bw_addtocart_and_checkout) * INTERVAL '1 minute')
        WHEN vt.purchase_time IS NOT NULL THEN 
            vt.purchase_time - ((ca.avg_time_bw_visit_and_addtocart + ca.avg_time_bw_addtocart_and_checkout + ca.avg_time_bw_checkout_and_purchase) * INTERVAL '1 minute')
    END
FROM valid_timestamps vt
JOIN customer_avg_event_time ca ON vt.customer_id = ca.customer_id
WHERE se.session_id = vt.session_id
AND se.event_type = 'visit'
AND se.timestamp IS NULL;

-- Update add_to_cart timestamps
UPDATE session_events se
SET timestamp = 
    CASE 
        WHEN vt.visit_time IS NOT NULL THEN 
            vt.visit_time + (ca.avg_time_bw_visit_and_addtocart * INTERVAL '1 minute')
        WHEN vt.checkout_time IS NOT NULL THEN 
            vt.checkout_time - (ca.avg_time_bw_addtocart_and_checkout * INTERVAL '1 minute')
        WHEN vt.purchase_time IS NOT NULL THEN 
            vt.purchase_time - ((ca.avg_time_bw_addtocart_and_checkout + ca.avg_time_bw_checkout_and_purchase) * INTERVAL '1 minute')
    END
FROM valid_timestamps vt
JOIN customer_avg_event_time ca ON vt.customer_id = ca.customer_id
WHERE se.session_id = vt.session_id
AND se.event_type = 'add_to_cart'
AND se.timestamp IS NULL;

-- Update checkout timestamps
UPDATE session_events se
SET timestamp = 
    CASE 
        WHEN vt.add_to_cart_time IS NOT NULL THEN 
            vt.add_to_cart_time + (ca.avg_time_bw_addtocart_and_checkout * INTERVAL '1 minute')
        WHEN vt.visit_time IS NOT NULL THEN 
            vt.visit_time + ((ca.avg_time_bw_visit_and_addtocart + ca.avg_time_bw_addtocart_and_checkout) * INTERVAL '1 minute')
        WHEN vt.purchase_time IS NOT NULL THEN 
            vt.purchase_time - (ca.avg_time_bw_checkout_and_purchase * INTERVAL '1 minute')
    END
FROM valid_timestamps vt
JOIN customer_avg_event_time ca ON vt.customer_id = ca.customer_id
WHERE se.session_id = vt.session_id
AND se.event_type = 'checkout'
AND se.timestamp IS NULL;

-- Update purchase timestamps
UPDATE session_events se
SET timestamp = 
    CASE 
        WHEN vt.checkout_time IS NOT NULL THEN 
            vt.checkout_time + (ca.avg_time_bw_checkout_and_purchase * INTERVAL '1 minute')
        WHEN vt.add_to_cart_time IS NOT NULL THEN 
            vt.add_to_cart_time + ((ca.avg_time_bw_addtocart_and_checkout + ca.avg_time_bw_checkout_and_purchase) * INTERVAL '1 minute')
        WHEN vt.visit_time IS NOT NULL THEN 
            vt.visit_time + ((ca.avg_time_bw_visit_and_addtocart + ca.avg_time_bw_addtocart_and_checkout + ca.avg_time_bw_checkout_and_purchase) * INTERVAL '1 minute')
    END
FROM valid_timestamps vt
JOIN customer_avg_event_time ca ON vt.customer_id = ca.customer_id
WHERE se.session_id = vt.session_id
AND se.event_type = 'purchase'
AND se.timestamp IS NULL;

-- Clean up by removing the created Temp Table named valid_timestamps
DROP TABLE valid_timestamps;

-- Check if we still have NULL timestamps
SELECT COUNT(*) 
FROM session_events 
WHERE timestamp IS NULL;

-- We still have 245 missing timestamps that is because some customers don't have all events, 
-- so their average time between events would be null.
-- See some sample fixed sessions
SELECT * 
FROM session_events
WHERE session_id IN (
    SELECT session_id FROM session_events 
    WHERE timestamp IS NOT NULL
    LIMIT 10
)
ORDER BY session_id, 
    CASE event_type
        WHEN 'visit' THEN 1
        WHEN 'add_to_cart' THEN 2
        WHEN 'checkout' THEN 3
        WHEN 'purchase' THEN 4
    END;

--ROLLBACK;
COMMIT;

-- We will now deal with the remaining 245 values.
-- We will calculate the global average duration between events.
-- Then fill those values.
BEGIN;

-- Calculate global average times
WITH global_averages AS (
    SELECT 
        ROUND(AVG(avg_time_bw_visit_and_addtocart), 0) AS global_avg_visit_to_cart,
        ROUND(AVG(avg_time_bw_addtocart_and_checkout), 0) AS global_avg_cart_to_checkout,
        ROUND(AVG(avg_time_bw_checkout_and_purchase), 0) AS global_avg_checkout_to_purchase
    FROM customer_avg_event_time
    WHERE 
        avg_time_bw_visit_and_addtocart IS NOT NULL AND
        avg_time_bw_addtocart_and_checkout IS NOT NULL AND
        avg_time_bw_checkout_and_purchase IS NOT NULL
),
-- Get baseline timestamps for each session with NULL values
session_base_times AS (
    SELECT 
        session_id,
        MIN(CASE WHEN event_type = 'visit' THEN timestamp END) AS visit_time,
        MIN(CASE WHEN event_type = 'add_to_cart' THEN timestamp END) AS cart_time,
        MIN(CASE WHEN event_type = 'checkout' THEN timestamp END) AS checkout_time,
        MIN(CASE WHEN event_type = 'purchase' THEN timestamp END) AS purchase_time,
        MIN(timestamp) AS base_time
    FROM session_events
    WHERE session_id IN (SELECT DISTINCT session_id FROM session_events WHERE timestamp IS NULL)
    GROUP BY session_id
)
-- Apply updates for each event type
UPDATE session_events se
SET timestamp = 
    CASE se.event_type
        WHEN 'visit' THEN
            COALESCE(
                sbt.visit_time,
                CASE 
                    WHEN sbt.cart_time IS NOT NULL THEN sbt.cart_time - (ga.global_avg_visit_to_cart * INTERVAL '1 minute')
                    WHEN sbt.checkout_time IS NOT NULL THEN sbt.checkout_time - ((ga.global_avg_visit_to_cart + ga.global_avg_cart_to_checkout) * INTERVAL '1 minute')
                    WHEN sbt.purchase_time IS NOT NULL THEN sbt.purchase_time - ((ga.global_avg_visit_to_cart + ga.global_avg_cart_to_checkout + ga.global_avg_checkout_to_purchase) * INTERVAL '1 minute')
                    ELSE sbt.base_time
                END
            )
        WHEN 'add_to_cart' THEN
            COALESCE(
                sbt.cart_time,
                CASE 
                    WHEN sbt.visit_time IS NOT NULL THEN sbt.visit_time + (ga.global_avg_visit_to_cart * INTERVAL '1 minute')
                    WHEN sbt.checkout_time IS NOT NULL THEN sbt.checkout_time - (ga.global_avg_cart_to_checkout * INTERVAL '1 minute')
                    WHEN sbt.purchase_time IS NOT NULL THEN sbt.purchase_time - ((ga.global_avg_cart_to_checkout + ga.global_avg_checkout_to_purchase) * INTERVAL '1 minute')
                    ELSE sbt.base_time + (ga.global_avg_visit_to_cart * INTERVAL '1 minute')
                END
            )
        WHEN 'checkout' THEN
            COALESCE(
                sbt.checkout_time,
                CASE 
                    WHEN sbt.cart_time IS NOT NULL THEN sbt.cart_time + (ga.global_avg_cart_to_checkout * INTERVAL '1 minute')
                    WHEN sbt.visit_time IS NOT NULL THEN sbt.visit_time + ((ga.global_avg_visit_to_cart + ga.global_avg_cart_to_checkout) * INTERVAL '1 minute')
                    WHEN sbt.purchase_time IS NOT NULL THEN sbt.purchase_time - (ga.global_avg_checkout_to_purchase * INTERVAL '1 minute')
                    ELSE sbt.base_time + ((ga.global_avg_visit_to_cart + ga.global_avg_cart_to_checkout) * INTERVAL '1 minute')
                END
            )
        WHEN 'purchase' THEN
            COALESCE(
                sbt.purchase_time,
                CASE 
                    WHEN sbt.checkout_time IS NOT NULL THEN sbt.checkout_time + (ga.global_avg_checkout_to_purchase * INTERVAL '1 minute')
                    WHEN sbt.cart_time IS NOT NULL THEN sbt.cart_time + ((ga.global_avg_cart_to_checkout + ga.global_avg_checkout_to_purchase) * INTERVAL '1 minute')
                    WHEN sbt.visit_time IS NOT NULL THEN sbt.visit_time + ((ga.global_avg_visit_to_cart + ga.global_avg_cart_to_checkout + ga.global_avg_checkout_to_purchase) * INTERVAL '1 minute')
                    ELSE sbt.base_time + ((ga.global_avg_visit_to_cart + ga.global_avg_cart_to_checkout + ga.global_avg_checkout_to_purchase) * INTERVAL '1 minute')
                END
            )
    END
FROM session_base_times sbt, global_averages ga
WHERE se.session_id = sbt.session_id
AND se.timestamp IS NULL;

-- Check for any remaining nulls
SELECT COUNT(*) 
FROM session_events 
WHERE timestamp IS NULL;

-- Dropping Temp Table
DROP TABLE customer_avg_event_time;

-- ROLLBACK;
COMMIT;

-- All missing timestamps have been dealt with.

-- Checking the update results
SELECT *
FROM session_events
WHERE session_id IN (843, 1969, 2119, 11090, 16524)
ORDER BY timestamp ASC;

-- At the time of fixing the wrong event order we skipped those sessions where timestamps were missing,
-- so we have to correct order for those events as well.


-- Checking out number of such sessions.
-- For example: purchase event shows timing of add_to_cart event and vice-versa.
DROP TABLE IF EXISTS wrong_order_sessions;
CREATE TEMP TABLE wrong_order_sessions AS
SELECT DISTINCT(session_id) AS session_id
FROM(
	SELECT *,
		CASE
			WHEN event_type = 'visit' THEN 1
			WHEN event_type = 'add_to_cart' THEN 2
			WHEN event_type = 'checkout' THEN 3
			WHEN event_type = 'purchase' THEN 4
		END AS event_order,
		ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY timestamp) AS time_organize  
	FROM session_events
	WHERE session_id NOT IN (SELECT session_id FROM session_events WHERE timestamp IS NULL)
	)
WHERE event_order != time_organize;

-- Updating the order of events in these sessions 
BEGIN;

WITH wrong_order AS (
    SELECT 
        session_id,
        timestamp,
        event_type AS original_event_type,  -- Store original event type
        ROW_NUMBER() OVER(
            PARTITION BY session_id 
            ORDER BY timestamp
        ) as event_order
    FROM session_events
    WHERE session_id IN (SELECT session_id FROM wrong_order_sessions)
)
UPDATE session_events AS se
SET event_type = CASE 
                    WHEN wo.event_order = 1 THEN 'visit'
                    WHEN wo.event_order = 2 THEN 'add_to_cart'
                    WHEN wo.event_order = 3 THEN 'checkout'
                    WHEN wo.event_order = 4 THEN 'purchase'
                END
FROM wrong_order AS wo
WHERE se.session_id = wo.session_id
    AND se.timestamp = wo.timestamp
    AND se.event_type = wo.original_event_type;  

-- ROLLBACK;
COMMIT;

-- Session_events is now clean
-- Query first 20 sessions
SELECT *
FROM session_events
WHERE session_id < 20
ORDER BY session_id, timestamp;
