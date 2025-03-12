-- Exploratory Data Analysis

SELECT COUNT(*)
FROM transact_data_staging;

SELECT*
FROM users_data_staging2;

SELECT*
FROM cards_data_staging;

-- Looking at max and min amount for a transaction 
	-- positive amount represents incoming transactions or income
    -- negative amount represents outgoing transactions or expense

-- Negative amounts are expenses from a debit card while positive amounts are expenses from a credit card
SELECT MAX(amount)
FROM financial_transactions.transact_data_staging; -- (2807.05)

SELECT MIN(amount)
FROM financial_transactions.transact_data_staging; -- (-500.00)

-- making sure the 'types' align within each dataset
DESCRIBE financial_transactions.transact_data_staging;
DESCRIBE financial_transactions.users_data_staging2;
DESCRIBE financial_transactions.cards_data_staging;

-- the duration of the dataset
SELECT MIN(`date`), MAX(`date`)
FROM financial_transactions.transact_data_staging; -- (1/01/2010 - 1/30/2010)


SELECT id, SUM(amount)
FROM financial_transactions.transact_data_staging
GROUP BY id
ORDER BY 2 DESC;

SELECT*
FROM financial_transactions.transact_data_staging
WHERE id = 7512096; -- Results: two transactions in amount of $2807.05 at the same time, online transactions, (suspicious)

SELECT*
FROM financial_transactions.transact_data_staging
WHERE id = 7517440; -- Results: two transactions in amount of $2680.80 at the same time,swipe transactions, (could be suspicious)

SELECT id, AVG(amount), SUM(amount)
FROM financial_transactions.transact_data_staging
GROUP BY id
ORDER BY 2 DESC;

SELECT COUNT(DISTINCT id), COUNT(DISTINCT client_id), COUNT(DISTINCT(card_id))
FROM financial_transactions.transact_data_staging; -- Results: 1083 clients

SELECT COUNT(DISTINCT client_id)
FROM financial_transactions.users_data_staging2; -- Results: 2000 clients

-- ALTER TABLE financial_transactions.transact_data_staging
-- RENAME COLUMN id to client_id;
-- I MIGHT NEED TO RENAME SOME OF THE COLUMNS

SELECT*
FROM financial_transactions.transact_data_staging
WHERE amount < 0;



WITH amount_per_state (merchant_state, days, total_amount) AS -- 1st CTE
( -- This CTE calculates the total amount spent by merchants in each state for each day.
-- (merchant_state, days, amount) are aliases for columns
SELECT merchant_state, DAY(`date`), SUM(amount) -- here we grab the data
FROM financial_transactions.transact_data_staging -- here we specify where to get it
GROUP BY merchant_state, DAY(`date`) -- here we specify how to group it
), State_Day_Rank AS -- 2nd CTE
(SELECT*, -- 'Dense_Rank()' unlike 'Rank()' when there is a tie it gives one of the rows 1 and the other 2 
DENSE_RANK() OVER (PARTITION BY days ORDER BY total_amount DESC) AS Ranking
FROM amount_per_state -- 'GROUP BY' always involves aggregation (e.g. SUM(), COUNT() etc) and 'PARTITION BY' has no aggregation; window functions are applied row by row within partitions.
WHERE merchant_state IS NOT NULL AND merchant_state != ''
)
SELECT*
FROM State_Day_Rank
WHERE Ranking <= 5;
-- Results: changing the 'Ranking <= 1'; it seems California has the most amount of transactions everyday for all 30 days
-- Results: changing the 'Ranking = 2'; it seems Texas and NY close in terms of 2nd place day to day.


-- Looking at the top 10 states with the most amount of money in terms of transactions in the whole month of January
WITH Top_States (merchant_state, months, total_amount) AS
(
SELECT merchant_state, MONTH(`date`), SUM(amount)
FROM financial_transactions.transact_data_staging
GROUP BY merchant_state, MONTH(`date`)
), State_Month_Rank AS 
(SELECT*, 
DENSE_RANK() OVER (PARTITION BY months ORDER BY total_amount DESC) AS Ranking
FROM Top_States
WHERE merchant_state IS NOT NULL AND merchant_state != ''
)
SELECT*
FROM State_Month_Rank
WHERE Ranking <= 10;

-- Looking at the top 10 states with the most number of transactions in the whole month of January
WITH Top_States_Count (merchant_state, months, total_count) AS
(
SELECT merchant_state, MONTH(`date`), COUNT(*) AS Total_transactions
FROM financial_transactions.transact_data_staging
GROUP BY merchant_state, MONTH(`date`)
), State_Month_Count AS 
(SELECT*, 
DENSE_RANK() OVER (PARTITION BY months ORDER BY total_count DESC) AS Ranking
FROM Top_States_Count
WHERE merchant_state IS NOT NULL AND merchant_state != ''
)
SELECT*
FROM State_Month_Count
WHERE Ranking <= 10;

SELECT*
FROM financial_transactions.transact_data_staging t1
LEFT JOIN financial_transactions.cards_data_staging t2
ON t1.client_id = t2.client_id;

-- checking if any of the cards were on the dark web
SELECT*
FROM financial_transactions.transact_data_staging t1
LEFT JOIN financial_transactions.cards_data_staging t2
ON t1.client_id = t2.client_id
WHERE card_on_dark_web = 'Yes';

-- this finds the total amount for all transactions separated by card type
SELECT card_type, SUM(amount) AS total_amount
FROM financial_transactions.transact_data_staging t1
LEFT JOIN financial_transactions.cards_data_staging t2
ON t1.client_id = t2.client_id
GROUP BY card_type;

-- this finds the total amount for all transactions separated by card brand
SELECT card_brand, SUM(amount) AS total_amount
FROM financial_transactions.transact_data_staging t1
LEFT JOIN financial_transactions.cards_data_staging t2
ON t1.client_id = t2.client_id
GROUP BY card_brand;

ALTER TABLE cards_data_staging
RENAME column id to card_id; -- noticed that some of the Column Names didn't match across the different Tables

ALTER TABLE users_data_staging2
RENAME column id to client_id;

SELECT*
FROM cards_data_staging;

SELECT*
FROM users_data_staging2;

-- joining two tables (transact_data_staging and users_data_staging2)
SELECT*
FROM financial_transactions.transact_data_staging 
LEFT JOIN financial_transactions.users_data_staging2
ON transact_data_staging.client_id = users_data_staging2.client_id;

-- this finds the total amount for all transactions separated by gender
SELECT gender, SUM(amount) AS total_amount
FROM financial_transactions.transact_data_staging 
LEFT JOIN financial_transactions.users_data_staging2
ON transact_data_staging.client_id = users_data_staging2.client_id
GROUP BY gender;

-- This CTE calculates the total amount for all transactions separated by the hour
WITH amount_per_hour(hours, total_amount) AS
(
SELECT HOUR(`date`), SUM(amount)
FROM financial_transactions.transact_data_staging
GROUP BY HOUR(`date`)
)
SELECT*
FROM amount_per_hour;

-- finding the average amount and adding 3 standard deviations
SELECT AVG(amount) + 3*STD(amount) 
FROM financial_transactions.transact_data_staging; -- Result: 292.98

-- finding the clients or users who spent more than 3 standard deviations over the average amount
SELECT client_id, card_id, amount
FROM financial_transactions.transact_data_staging
WHERE amount > 
(SELECT AVG(amount) + 3*STD(amount) 
FROM financial_transactions.transact_data_staging);

-- finding the number of transactions for each card type: debit(prepaid), credit, debit
SELECT card_type, COUNT(*) AS transaction_count
FROM financial_transactions.transact_data_staging LEFT JOIN
financial_transactions.cards_data_staging
ON transact_data_staging.card_id = cards_data_staging.card_id
GROUP BY card_type;


-- finding transactions with matching amounts
SELECT client_id, amount, COUNT(*) AS transaction_count
FROM financial_transactions.transact_data_staging
GROUP BY client_id, amount
HAVING transaction_count > 1;

SELECT*
FROM transact_data_staging
WHERE client_id = 1385 AND amount = 140.00;

SELECT*
FROM cards_data_staging
WHERE client_id = 1385;

-- grouped the 'current_age' in to 5 categories and looked at the total_spent and average spent per age group
SELECT CASE 
        WHEN current_age BETWEEN 18 AND 24 THEN '18-24'
        WHEN current_age BETWEEN 25 AND 34 THEN '25-34'
        WHEN current_age BETWEEN 35 AND 45 THEN '35-45'
        WHEN current_age BETWEEN 46 AND 60 THEN '46-60'
        ELSE '60+' 
        END AS age_group,
        SUM(amount) AS total_spent,
        AVG(amount) AS average_spent
FROM financial_transactions.transact_data_staging t1
LEFT JOIN financial_transactions.users_data_staging2 t2
ON t1.client_id = t2.client_id
GROUP BY age_group;

SELECT MIN(current_age), MAX(current_age)
FROM financial_transactions.transact_data_staging t1 
LEFT JOIN financial_transactions.users_data_staging2 t2
ON t1.client_id = t2.client_id;


-- Looking at all the duplicate rows and if so how many dupliates
WITH duplicate_transactions_cte AS
(
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY id, `date`, client_id, card_id, amount, use_chip, merchant_id, merchant_city, merchant_state) AS row_num
FROM financial_transactions.transact_data_staging
)
SELECT COUNT(*)
FROM duplicate_transactions_cte
WHERE row_num > 1;

SELECT*
FROM financial_transactions.transact_data_staging
WHERE id = 7475336;

-- The output includes both duplicated rows
WITH duplicate_transactions_cte AS (
SELECT *,
COUNT(*) OVER(
PARTITION BY id, `date`, client_id, card_id, amount, use_chip, merchant_id, merchant_city, merchant_state
) AS duplicate_count
FROM financial_transactions.transact_data_staging
)
SELECT *
FROM duplicate_transactions_cte
WHERE duplicate_count > 1;

