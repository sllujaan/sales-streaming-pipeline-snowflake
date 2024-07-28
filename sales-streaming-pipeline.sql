drop database if exists DEMO;
CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.PUBLIC;
USE SCHEMA DEMO.PUBLIC;



CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;


CREATE OR REPLACE STAGE raw_data_stage file_format = my_csv_format directory = (enable=true);


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
    last_modified timestamp,
    is_valid boolean
);


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


--alter pipe raw_data_load refresh;


create or replace task raw_data_load_task
warehouse=FIRST_WH
schedule='1 minute'
as
alter pipe raw_data_load refresh;

--alter task raw_data_load_task resume;
--execute task raw_data_load_task;


create or replace TABLE VALID_ROW_DATA (
	id NUMBER(38,0),
	customer_id NUMBER(38,0),
	customer_name VARCHAR(16777216),
	customer_age NUMBER(38,0),
	customer_country VARCHAR(16777216),
	product_id NUMBER(38,0),
	product_name VARCHAR(16777216),
	product_price NUMBER(38,0),
	product_quantity NUMBER(38,0),
	order_date timestamp,
    last_modified timestamp
);


select *,
    (try_cast(id as int) is not null and
    try_cast(customer_id as int) is not null and
    try_cast(customer_age as int) is not null and
    try_cast(product_id as int) is not null and
    try_cast(product_price as decimal) is not null and
    try_cast(product_quantity as int) is not null and
    try_cast(order_date as date) is not null) and
    ((customer_name is not null and customer_name != '') or
    (customer_country is not null and customer_country != '') or
    (product_name is not null and product_name != ''))
    as __is_valid
from raw_data;






select $1, $2, $3, $4, $5 from @raw_data_stage/sales;

select * from raw_data where is_valid = false;

select * from customers;
select * from products;
select * from sales;
select * from sales_gold;


select 
    t1.id,
    t1.customer_id,
    t1.product_id,
    t1.product_quantity,
    t1.order_date,
    t2.name as "CUSTOMER_NAME",
    t3.name as "PRODUCT_NAEM",
    t3.price as "PRODUCT_PRICE"
from sales t1
left join customers t2
on t1.customer_id = t2.id
left join products t3
on t1.product_id = t3.id;





