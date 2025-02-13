DROP TABLE IF EXISTS products; 
CREATE TABLE products (
    id TEXT PRIMARY KEY,
    sku TEXT,
    item_description TEXT,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,    
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone DEFAULT NULL
);

DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    invoice_id TEXT,
    receipt_id TEXT UNIQUE, 
    product_id TEXT,
    date_id uuid,
    customer_id TEXT,
    region_id uuid,
    sales_quantity INTEGER,
    unit_price DOUBLE PRECISION,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,    
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone DEFAULT NULL
);

DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    id TEXT PRIMARY KEY,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,    
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone DEFAULT NULL
);

DROP TABLE IF EXISTS region;
CREATE TABLE regions (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    region_name TEXT UNIQUE,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,    
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone DEFAULT NULL
);


DROP TABLE IF EXISTS date_dimension;
CREATE TABLE date_dimension
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    date_val DATE,
    full_day_description TEXT,
    day_of_week TEXT,
    calendar_month TEXT,
    calendar_year INTEGER,
    fiscal_month TEXT,
    holiday_indicator BOOLEAN,
    weekday_indicator BOOLEAN 
);

INSERT INTO date_dimension
    (date_val, full_day_description, day_of_week, calendar_month, calendar_year, fiscal_month, holiday_indicator, weekday_indicator)
SELECT
    day,
    rtrim(to_char(day, 'Month')) || to_char(day, ' DD, YYYY'),
    to_char(day, 'Day'),
    rtrim(to_char(day, 'Month')),
    date_part('year', day),
    'F' || to_char(day, 'YYYY-MM'),
    FALSE,
    CASE
        WHEN date_part('isodow', day) IN (6, 7) THEN FALSE
        ELSE TRUE
    END
FROM
    generate_series('2001-01-01'::date, '2025-12-31'::date, '1 day') day;