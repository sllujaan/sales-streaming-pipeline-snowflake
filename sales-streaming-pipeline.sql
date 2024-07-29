-- # Real-Time Data Pipeline and Warehouse for Sales Using Snowflake

-- This project demonstrates a comprehensive real-time data warehouse solution for processing and analyzing incoming sales data from raw files.

-- ## Project Overview

-- This project provides solutions for:

-- 1. **Streaming Data Pipelines**: Efficiently processing and transforming incoming sales data in real-time.
-- 2. **Data Warehouse**: Storing and managing the processed data for efficient querying and analysis.
-- 3. **Data Analysis**: Analyzing the stored data to derive meaningful insights.

-- ## Tools and Technologies Used

-- 1. **Snowflake**: Utilized for Streaming to handle real-time data processing.
-- 2. **SQL**: Employed for querying and managing data within the data warehouse.

-- ## ETL Life Cycle

-- The project implements the full ETL (Extract, Transform, Load) life cycle:

-- 1. **Extract**: Raw sales data is ingested from various sources.
-- 2. **Transform**: Data is processed and transformed in real-time using Snowflake Streaming.
-- 3. **Load**: Transformed data is loaded into the data warehouse for storage and further analysis.

-- This project showcases an end-to-end solution for real-time data warehousing, ensuring that sales data is processed, stored, and analyzed efficiently.



DROP DATABASE IF EXISTS DEMO;
CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.PUBLIC;
USE SCHEMA DEMO.PUBLIC;



--------------------------------------------STAGGING PART-----------------------------------------------------



-- file format for csv files
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

-- create a stage for the raw data
CREATE OR REPLACE STAGE raw_data_stage file_format = my_csv_format directory = (enable=true);

-- create a table for the raw data to store
create or replace TABLE RAW_DATA (
	id VARCHAR(16777216),
	customer_id VARCHAR(16777216),
	customer_name VARCHAR(16777216),
	customer_age VARCHAR(16777216),
	customer_country VARCHAR(16777216),
	product_id VARCHAR(16777216),
	product_name VARCHAR(16777216),
	product_price VARCHAR(16777216),
	product_quantity VARCHAR(16777216),
	order_date VARCHAR(16777216),
    last_modified timestamp,    -- additional
    is_valid boolean            -- additional
);


-- now create a pipe for raw data to load automatically
create or replace pipe raw_data_load
as
copy into RAW_DATA
from
(select
    $1,  -- id
    $2,  -- customer_id
    $3,  -- customer_name
    $4,  -- customer_age
    $5,  -- customer_country
    $6,  -- product_id
    $7,  -- product_name
    $8,  -- product_price
    $9,  -- product_quantity
    $10, -- order_date
    METADATA$FILE_LAST_MODIFIED,  -- last_modified
    
    (try_cast($1 as int) is not null and
    try_cast($2 as int) is not null and
    try_cast($4 as int) is not null and
    try_cast($6 as int) is not null and
    try_cast($8 as decimal) is not null and
    try_cast($9 as int) is not null and
    try_cast($10 as date) is not null) and
    (($3 is not null and $3 != '') or
    ($5 is not null and $5 != '') or
    ($7 is not null and $7 != ''))    -- is_valid
from @raw_data_stage/sales);


-- refresh the pipe so that it will start ingesting data from the raw files
alter pipe raw_data_load refresh;

-- create a task to refresh the pipe created above
create or replace task raw_data_load_task
warehouse=FIRST_WH
schedule='1 minute'
as
alter pipe raw_data_load refresh;

--alter task raw_data_load_task resume;
--execute task raw_data_load_task;




--------------------------------------------STREAMING PART-----------------------------------------------------


-- create a stream on raw data table
create or replace stream raw_data_stream on table raw_data;


-- create a view for the new customers
create or replace view new_customers
as
with cte as
(
    select
        customer_id,customer_name, customer_age, customer_country,
        row_number() over (partition by customer_id order by last_modified desc) as rn
    
    from raw_data_stream where is_valid = true
)
select customer_id,customer_name, customer_age, customer_country from cte where rn = 1;


-- create a view for the new products
create or replace view new_products
as
with cte as
(
    select
        product_id, product_name, product_price,
        row_number() over (partition by product_id order by last_modified desc) as rn
    
    from raw_data_stream where is_valid = true
)
select product_id, product_name, product_price from cte where rn = 1;


-- create a view for the new sales
create or replace view new_sales
as
with cte as
(
    select
        id, customer_id, product_id, product_quantity, order_date,
        row_number() over (partition by id order by last_modified desc) as rn
    
    from raw_data_stream where is_valid = true
)
select id, customer_id, product_id, product_quantity, order_date from cte where rn = 1;


------------------create final tables-----------------------------
create or replace table customers(
    id int primary key not null AUTOINCREMENT,
    name VARCHAR(16777216),
    age int,
    country VARCHAR(16777216)
);

create or replace table products(
    id int primary key not null AUTOINCREMENT,
    name VARCHAR(16777216),
    price NUMBER(38,0)
);

create or replace table sales(
    id int primary key not null AUTOINCREMENT,
    customer_id int not null,
    product_id int not null,
    product_quantity NUMBER(38,0) not null,
    order_date timestamp not null,
    constraint fkey_customer_id foreign key (customer_id) references customers (id) enforced,
    constraint fkey_product_id foreign key (product_id) references products (id) enforced
);



-- create a procecure to load the data to final tables created above
create or replace procedure load_to_warehouse()
    returns VARCHAR(16777216)
as
$$
begin
    begin transaction;

        merge into customers t1
            using new_customers t2
            on t1.id = t2.customer_id
            when matched
                then update set t1.name = t2.customer_name, t1.age = t2.customer_age, t1.country = t2.customer_country
            when not matched
                then insert (id, name, age, country) values (t2.customer_id, t2.customer_name, t2.customer_age, t2.customer_country);
                
        merge into products t1
            using new_products t2
            on t1.id = t2.product_id
            when matched
                then update set t1.name = t2.product_name, t1.price = t2.product_price
            when not matched
                then insert (id, name, price) values (t2.product_id, t2.product_name, t2.product_price);

        merge into sales t1
            using new_sales t2
            on t1.id = t2.id
            when matched
                then update set
                    t1.customer_id = t2.customer_id, t1.product_id = t2.product_id,
                    t1.product_quantity = t2.product_quantity, t1.order_date = t2.order_date
            when not matched
                then insert (id, customer_id, product_id, product_quantity, order_date)
                    values (t2.id, t2.customer_id, t2.product_id, t2.product_quantity, t2.order_date);
        
    commit;
    return 'done';
end;
$$;



-- create a task to run the procedure created above
create or replace task task_load_to_warehouse
warehouse=FIRST_WH
when SYSTEM$STREAM_HAS_DATA('raw_data_stream')
as
    call load_to_warehouse();


----------------- add task dependencies-------------
alter task task_load_to_warehouse add after raw_data_load_task; 
alter task task_load_to_warehouse resume;
alter task raw_data_load_task resume;



-- That's it we have just create a streaming pipeline for our sales data




------------------------Analytical queries---------------------------------



--------sales by year and month----------------
select
        YEAR(sales.order_date) as year,
        MONTH(sales.order_date) as month,
        SUM(sales.product_quantity * products.price) as total_sales,
        count(*) as total_orders
    from sales
        left join products
            on sales.product_id = products.id
    group by year, month
    order by year, month;


--------sales by product----------------
select
        products.name as product_name,
        SUM(sales.product_quantity * products.price) as total_sales,
        count(*) as total_orders
    from sales
        left join products
            on sales.product_id = products.id
    group by product_name
    order by product_name;



--------sales by country----------------
select
        customers.country as country,
        SUM(sales.product_quantity * products.price) as total_sales,
        count(*) as total_orders
    from sales
        left join customers
            on sales.customer_id = customers.id
        left join products
            on sales.product_id = products.id
    group by country
    order by country;



--------sales by year----------------
select
        YEAR(sales.order_date) as year,
        SUM(sales.product_quantity * products.price) as total_sales,
        count(*) as total_orders
    from sales
        left join products
            on sales.product_id = products.id
    group by year
    order by year;


--------sales by month----------------
select
        MONTH(sales.order_date) as month,
        SUM(sales.product_quantity * products.price) as total_sales,
        count(*) as total_orders
    from sales
        left join products
            on sales.product_id = products.id
    group by month
    order by month;


--------sales by month and country----------------
select
        MONTH(sales.order_date) as month,
        customers.country as country,
        SUM(sales.product_quantity * products.price) as total_sales,
        count(*) as total_orders
    from sales
        left join products
            on sales.product_id = products.id
        left join customers
            on sales.customer_id = customers.id
    group by month, country
    order by month, country;







----------------- use full queries---------------------
--execute task task_load_to_warehouse;
--call load_to_warehouse();
--alter pipe raw_data_load refresh;
--select SYSTEM$STREAM_HAS_DATA('raw_data_stream');
--select * from raw_data_stream;
--select * from raw_data;
--select * from customers;
--select * from products;
--select * from sales;