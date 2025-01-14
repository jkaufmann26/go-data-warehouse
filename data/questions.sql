-- QUESTION 1: What are our top 5 most popular products?--
SELECT products.item_description, SUM( sales_quantity ) 
AS quantity FROM sales 
LEFT JOIN products ON sales.product_id = products.id 
GROUP BY products.item_description 
ORDER BY quantity DESC LIMIT 5;

-- QUESTION 2: What was the total revenue brought in each Month?--
SELECT date_dimension.calendar_month, date_dimension.calendar_year, SUM(sales_quantity * unit_price) AS revenue
FROM sales 
INNER JOIN date_dimension 
ON sales.date_id = date_dimension.id 
GROUP BY date_dimension.calendar_month, date_dimension.calendar_year
ORDER BY revenue DESC;

--QUESTION 3: What is the average number of items on an invoice, broken down by Month?--
SELECT DISTINCT ON (date_dimension.calendar_month, date_dimension.calendar_year) date_dimension.calendar_month, date_dimension.calendar_year, avg(count) OVER (PARTITION BY date_dimension.calendar_month) 
FROM (SELECT date_id,invoice_id,COUNT(*) FROM sales GROUP BY invoice_id,date_id)
INNER JOIN date_dimension 
ON date_id = date_dimension.id 
ORDER BY date_dimension.calendar_month, date_dimension.calendar_year,count DESC;

--QUESTION 4: Who are our top 5 customers (in terms of $ spent) and what are the top 5 products they each order?--
WITH top_customers AS (SELECT customer_id, SUM(sales_quantity * unit_price) AS total_spent 
FROM sales
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 5)
SELECT customer_id, products.item_description, SUM(sales_quantity) as q 
FROM sales 
    INNER JOIN products on sales.product_id = products.id
WHERE customer_id IN (
    SELECT customer_id FROM top_customers
)
GROUP BY customer_id, products.item_description
ORDER BY q DESC LIMIT 5;