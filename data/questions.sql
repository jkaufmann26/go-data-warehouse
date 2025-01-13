-- QUESTION 1: What are our top 5 most popular products?--
SELECT products.item_description, SUM( sales_quantity ) 
AS quantity FROM sales 
LEFT JOIN products ON sales.product_key = products.id 
GROUP BY products.item_description 
ORDER BY quantity DESC LIMIT 5;

-- QUESTION 2: What was the total revenue brought in each Month?--
SELECT date_dimension.calendar_month, date_dimension.calendar_year, SUM(sales_quantity * unit_price) AS revenue
FROM sales 
INNER JOIN date_dimension 
ON sales.date_key = date_dimension.id 
GROUP BY date_dimension.calendar_month, date_dimension.calendar_year
ORDER BY revenue DESC;

--QUESTION 3: What is the average number of items on an invoice, broken down by Month?--
SELECT date_dimension.calendar_month, date_dimension.calendar_year, AVG(COUNT(product_key))
FROM sales 
INNER JOIN date_dimension 
ON sales.date_key = date_dimension.id 
GROUP BY invoice_id, date_dimension.calendar_month, date_dimension.calendar_year
ORDER BY revenue DESC;

--QUESTION 4: Who are our top 5 customers (in terms of $ spent) and what are the top 5 products they each order?--
SELECT customer_key AS key, SUM(sales_quantity * unit_price), top_products as total_spent
FROM sales INNER JOIN (


) top_products
GROUP BY customer_key
ORDER BY total_spent DESC
LIMIT 6;


    SELECT products.item_description
    FROM sales INNER JOIN products on sales.product_key = products.id
    WHERE customer_key = '16210'
    GROUP BY products.item_description   
    ORDER BY SUM(sales_quantity * unit_price) DESC
    LIMIT 5