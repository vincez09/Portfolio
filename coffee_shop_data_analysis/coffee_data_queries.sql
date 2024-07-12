DROP TABLE IF EXISTS coffee_data

CREATE TABLE coffee_data (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date VARCHAR(50),
    transaction_time TIME,
    store_id INT,
    store_location VARCHAR(50),
    product_id INT,
    transaction_qty NUMERIC,
    unit_price NUMERIC,
    Total_Bill NUMERIC,
    product_category VARCHAR(100),
    product_type VARCHAR(100),
    product_detail VARCHAR(250),
    size VARCHAR(50),
    month_name VARCHAR(25),
    day_name VARCHAR(25),
    hour INT,
    month INT,
    day_of_week INT
);

-- Convert transaction_date from VARCHAR to DATE
ALTER TABLE coffee_data ADD COLUMN transaction_date_new DATE

UPDATE coffee_data
SET transaction_date_new = TO_DATE(transaction_date, 'YYYY-MM-DD')

ALTER TABLE coffee_data DROP COLUMN transaction_date

ALTER TABLE coffee_data RENAME COLUMN transaction_date_new TO transaction_date

SELECT *
FROM coffee_data
LIMIT 5

-- Checking null values in the table
SELECT
    COUNT(*) AS total_rows,
    COUNT(transaction_id) AS transaction_id_rows,
    COUNT(transaction_date) AS transaction_date_rows,
    COUNT(transaction_time) AS transaction_time_rows,
    COUNT(store_id) AS store_id_rows,
    COUNT(store_location) AS store_location_rows,
    COUNT(product_id) AS product_id_rows,
    COUNT(transaction_qty) AS transaction_qty_rows,
    COUNT(unit_price) AS unit_price_rows,
    COUNT(total_bill) AS total_bill_rows,
    COUNT(product_category) AS product_category_rows,
    COUNT(product_type) AS product_type_rows,
    COUNT(product_detail) AS product_detail_rows,
    COUNT(size) AS size_rows,
    COUNT(month_name) AS month_name_rows,
    COUNT(day_name) AS day_name_rows,
    COUNT(hour) AS hour_rows,
    COUNT(month) AS month_rows,
    COUNT(day_of_week) AS day_of_week_rows
FROM 
    coffee_data;

-- Total sales per year
SELECT 
	EXTRACT(YEAR FROM transaction_date) AS year,
	SUM(total_bill) AS total_sales
FROM coffee_data
GROUP BY year
ORDER BY year

-- Compare sales of each coffee shop
SELECT
	store_location,
	SUM(total_bill) AS total_sales
FROM coffee_data
GROUP BY store_location


-- Get total sales per product_category and contribution to total sales
SELECT
	product_category,
	SUM(total_bill) AS total_sales,
	ROUND(SUM(total_bill) * 100.0 / SUM(SUM(total_bill)) OVER (),2) AS sales_contribution_percentage
FROM coffee_data
GROUP BY product_category
ORDER BY total_sales DESC

--Get total sales and total qty sold per product_id
SELECT
	product_id,
	product_detail,
	product_category,
	SUM(total_bill) AS total_sales,
	SUM(transaction_qty) AS total_qty
FROM coffee_data
GROUP BY product_id, product_detail, product_category
ORDER BY total_sales DESC

-- Total sales per month
SELECT
	month,
	SUM(total_bill) AS total_sales
FROM coffee_data
GROUP BY month
ORDER BY month

--Top 3 sales per month per product
WITH monthly_sales_rank AS 
	(SELECT
		month,
		product_detail,
		SUM(total_bill) AS total_sales,
		RANK() OVER (PARTITION BY month ORDER BY SUM(total_bill) DESC) AS monthly_rank
	FROM coffee_data
	GROUP BY month, product_detail
	ORDER BY month, total_sales DESC)
SELECT *
FROM monthly_sales_rank
WHERE monthly_rank >= 1 AND monthly_rank <= 3

--Top 3 Products for each store location
WITH ranked_products AS (
    SELECT
        store_location,
        product_detail,
        SUM(total_bill) AS total_sales,
        ROW_NUMBER() OVER (PARTITION BY store_location ORDER BY SUM(total_bill) DESC) AS rank
    FROM
        coffee_data
    GROUP BY
        store_location, product_detail
)
SELECT
    store_location,
    product_detail,
    total_sales
FROM
    ranked_products
WHERE
    rank <= 3
ORDER BY
    store_location, rank;

--Average daily transactions per month
SELECT
	month,
	ROUND(COUNT(*)/EXTRACT(DAYS FROM MAX(transaction_date))) AS avg_daily_transactions
FROM coffee_data
GROUP BY month
ORDER BY month

--Total sales daily
SELECT 
	day_name,
	SUM(total_bill) AS daily_total_sales
FROM coffee_data
GROUP BY day_name
ORDER BY
	CASE
		WHEN day_name = 'Monday' THEN 1
		WHEN day_name = 'Tuesday' THEN 2
		WHEN day_name = 'Wednesday' THEN 3
		WHEN day_name = 'Thursday' THEN 4
		WHEN day_name = 'Friday' THEN 5
		WHEN day_name = 'Saturday' THEN 6
		WHEN day_name = 'Sunday' THEN 7
	END

--Query month sales change rate for each store location
WITH monthly_sales AS (
    SELECT
        store_location,
        month_name,
        SUM(total_bill) AS total_sales,
        LAG(SUM(total_bill)) OVER (PARTITION BY store_location ORDER BY month) AS prev_month_sales
    FROM
        coffee_data
    GROUP BY
        store_location, month_name, month
)
SELECT
    store_location,
    month_name,
    total_sales,
    prev_month_sales,
    ROUND((total_sales - prev_month_sales) * 100.0 / prev_month_sales,2) AS sales_change_percentage
FROM
    monthly_sales
WHERE
    prev_month_sales IS NOT NULL
ORDER BY
    store_location, month_name;