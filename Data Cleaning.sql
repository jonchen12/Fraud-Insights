

-- DATA CLEANING
	-- Goals --
	-- 1. Remove Duplicates
	-- 2. Standardize the Data
	-- 3. Null Values or blank values
	-- 4. Remove Any Columns


-- DATA CLEANING for transact_data file
-- creating a copy of transactions dataset
CREATE TABLE transact_data_staging
LIKE transact_data;

SELECT *
FROM transact_data_staging;

INSERT transact_data_staging
SELECT *
FROM transact_data;

ALTER TABLE transact_data_staging
MODIFY `date` DATETIME;


-- understanding the data 
DESCRIBE transact_data_staging;

SELECT *
FROM transact_data_staging;

SELECT
COUNT(*)
FROM transact_data_staging; -- 96733

SELECT 
COUNT(*)
FROM transact_data_staging
WHERE merchant_city = 'ONLINE'; -- 10677

SELECT MIN(`date`) AS earliest_date, MAX(`date`) AS latest_date
FROM transact_data_staging;


-- Checking for Duplicates
with duplicate_transact_cte AS
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY id, client_id, card_id, amount, use_chip, merchant_id) AS row_num
FROM transact_data_staging
)
SELECT*
FROM duplicate_transact_cte
WHERE row_num > 1; -- found duplicates rows 

SELECT*
FROM transact_data_staging
WHERE card_id = '94';
-- I decided to leave the duplicate rows as they could reveal important details about fradualent activity

-- Standarizing the Data
SELECT DISTINCT(use_chip)
FROM transact_data_staging;

SELECT DISTINCT(merchant_city)
FROM transact_data_staging;

SELECT DISTINCT(merchant_state)
FROM transact_data_staging;


UPDATE transact_data_staging
SET amount = CAST(REPLACE(TRIM(amount), '$', '') AS DECIMAL(10, 2));

UPDATE transact_data_staging
SET zip = REPLACE(TRIM(zip), '.0', '');

SELECT * 
FROM transact_data_staging
WHERE LENGTH(zip) = 4 AND zip NOT LIKE '%[^0-9]%';
 -- discovered that the zipcodes that were supposed to have a 0 in the front of the zipcode was missing 
	-- that's why they were only appearing with 4 digits


UPDATE transact_data_staging
SET zip = CONCAT('0', zip)
WHERE LENGTH(zip) = 4 AND zip NOT LIKE '%[^0-9]%'; -- here I added the 0 to all the zipcodes with only 4 digits


-- Checking for Null Values or blank values
SELECT*
FROM transact_data_staging
WHERE merchant_state = '' AND merchant_city != 'ONLINE';
	-- There only blank values for merchant_state when the transactions are online

SELECT*
FROM transact_data_staging
WHERE zip = '' AND merchant_city != 'ONLINE';
	-- Not only is zipcode is blank when transaction are online BUT
		-- also when transaction are NOT in the USA
SELECT*
FROM transact_data_staging
WHERE merchant_city = 'ONLINE';
	-- All online transaction have blank zipcodes and merchant cities
    
    
-- 4. Remove Any Columns
SELECT*
FROM transact_data_staging;








-- DATA CLEANING for cards_data file
-- creating a copy of cards dataset
CREATE TABLE cards_data_staging

LIKE cards_data;

INSERT cards_data_staging
SELECT*
FROM cards_data;

SELECT*
FROM cards_data_staging;

-- understanding the data
DESCRIBE cards_data_staging;

SELECT
COUNT(*)
FROM cards_data_staging; -- 6146 

-- Checking for Duplicates
WITH duplicate_cards_cte AS 
(
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY id, client_id, card_brand, card_type, card_number, expires) AS row_num
FROM cards_data_staging
)
SELECT*
FROM duplicate_cards_cte
WHERE row_num > 1;

-- Standardizing the Data
SELECT*
FROM cards_data_staging;

SELECT DISTINCT(card_type)
FROM cards_data_staging;

SELECT DISTINCT(card_brand)
FROM cards_data_staging;

SELECT DISTINCT(client_id)
FROM cards_data_staging;

SELECT COUNT(DISTINCT cvv)
FROM cards_data_staging;

SELECT COUNT(DISTINCT card_number)
FROM cards_data_staging; -- the number of distinct card_number = rows in dataset

SELECT DISTINCT(has_chip)
FROM cards_data_staging;

UPDATE cards_data_staging
SET credit_limit = CAST(REPLACE(TRIM(credit_limit), '$', '') AS DECIMAL(15, 2));

-- Checking for Null Values or blank values
SELECT*
FROM users_data_staging2
WHERE address is NULL 
	or total_debt is NULL 
    or credit_score is NULL;

SELECT*
FROM cards_data_staging
WHERE cvv is NULL or cvv = ''
	or card_number is NULL or card_number = ''
    or credit_limit is NULL or credit_limit = ''; -- found 4 rows where cvv is 0
												-- leaving in case it correalated with fradulent activity
    
    
-- 4. Remove Any Columns
SELECT*
FROM cards_data_staging;
	-- found no columns to remove
    







-- DATA CLEANING for users_data file
-- creating a copy of users dataset
CREATE TABLE users_data_staging
LIKE users_data;

SELECT *
FROM users_data_staging;

INSERT users_data_staging
SELECT *
FROM users_data;


-- understanding the data 
DESCRIBE users_data_staging;
	-- following are text format that may need changing:
		-- per_capita_income, yearly_income_total_debt, total debt

SELECT *
FROM users_data_staging;

SELECT
COUNT(*)
FROM users_data_staging; -- 4000



-- Checking for Duplicates
with duplicate_users_cte AS
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY id, current_age, retirement_age, birth_year, gender, address, latitude, longitude) AS row_num
FROM users_data_staging
)

SELECT*
FROM duplicate_users_cte
WHERE row_num > 1; -- found duplicates rows 

SELECT COUNT(distinct(id))
FROM users_data_staging;
	-- found the number of distinct ids is 2000 which half of length of total rows
	-- It seems each user is duplicated once so in reality we only have 2000 distinct users


CREATE TABLE `users_data_staging2` (
  `id` int DEFAULT NULL,
  `current_age` int DEFAULT NULL,
  `retirement_age` int DEFAULT NULL,
  `birth_year` int DEFAULT NULL,
  `birth_month` int DEFAULT NULL,
  `gender` text,
  `address` text,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `per_capita_income` text,
  `yearly_income` text,
  `total_debt` text,
  `credit_score` int DEFAULT NULL,
  `num_credit_cards` int DEFAULT NULL,
  `row_num` int 							-- ADDED THIS COLUMN
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT* 
FROM users_data_staging2;

INSERT INTO users_data_staging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY id, current_age, retirement_age, birth_year, gender, address, latitude, longitude) AS row_num
FROM users_data_staging;

DELETE
FROM users_data_staging2 
WHERE row_num > 1; -- deleting all the duplicates in the new table users_data_staging2

SELECT* 
FROM users_data_staging2; -- check if duplicate is deleted which is



-- Standarizing the Data
SELECT DISTINCT(gender) -- checking for spacing/formatting
FROM users_data_staging2;

SELECT DISTINCT(address)
FROM users_data_staging2;

SELECT DISTINCT(birth_year)
FROM users_data_staging2;

UPDATE users_data_staging2
SET 
	per_capita_income = CAST(REPLACE(TRIM(per_capita_income), '$', '') AS DECIMAL(15,2)),
    yearly_income = CAST(REPLACE(TRIM(yearly_income), '$', '') AS DECIMAL(15, 2)),
    total_debt = CAST(REPLACE(TRIM(total_debt), '$', '') AS DECIMAL(15, 2));


SELECT*
FROM users_data_staging2;


-- Checking for Null Values or blank values
SELECT*
FROM users_data_staging2
WHERE address is NULL 
	or total_debt is NULL 
    or credit_score is NULL;

SELECT*
FROM users_data_staging2
WHERE address is NULL or address = ''
	or total_debt is NULL or total_debt = ''
    or credit_score is NULL or credit_score = ''; -- Found no null values
    

-- 4. Remove Any Columns
SELECT*
FROM users_data_staging2;

ALTER TABLE users_data_staging2
DROP COLUMN row_num;





