/*A stakeholder in the HR department wants to do some change-tracking but is concerned that the stream which was created for them gives them too much info they don’t care about.

Load in the parquet data and transform it into a table, then create a stream that will only show us changes to the DEPT and JOB_TITLE columns. 

You can find the parquet data here.

Execute the following commands:

UPDATE employees SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
UPDATE employees SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
UPDATE employees SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
UPDATE employees SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
UPDATE employees SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;*/

USE DATABASE FROSTY_DB;
USE SCHEMA FRIDAY;
USE WAREHOUSE COMPUTE_WH;

CREATE OR ALTER STAGE my_int_stage;

-- SnowSQL
.snowsql\config -> [connections.DEV]
snowsql -c DEV
PUT file://C:\temp\data\employees.parquet @my_int_stage;
-------------------------------------

LIST @my_int_stage;

--{"employee_id":1,"first_name":"Tabor","last_name":"Brader","email":"tbrader0@sfgate.com","street_num":2393,"street_name":"Orin","city":"Norfolk","postcode":"23509","country":"United States","country_code":"US","time_zone":"America/New_York","payroll_iban":"FR22 2876 2434 98Z3 WE1F CMWW T99","dept":"Business Development","job_title":"Structural Analysis Engineer","education":"Ohio Dominican College","title":"Ms","suffix":null}

CREATE OR REPLACE FILE FORMAT parquetformat TYPE = 'PARQUET';

SELECT
    $1 AS data,
    metadata$filename AS file_name,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM
    @my_int_stage (file_format => parquetformat);

CREATE OR REPLACE TABLE employees (
    employee_id NUMBER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    street_num NUMBER,
    street_name VARCHAR,
    city VARCHAR,
    postcode VARCHAR,
    country VARCHAR,
    country_code VARCHAR,
    time_zone VARCHAR,
    payroll_iban VARCHAR,
    dept VARCHAR,
    job_title VARCHAR,
    education VARCHAR,
    title VARCHAR,
    suffix VARCHAR,
    file_name VARCHAR,
    load_timestamp TIMESTAMP);


COPY INTO employees FROM (
SELECT 
    $1:employee_id::NUMBER,
    $1:first_name::VARCHAR,
    $1:last_name::VARCHAR,
    $1:email::VARCHAR,
    $1:street_num::NUMBER,
    $1:street_name::VARCHAR,
    $1:city::VARCHAR,
    $1:postcode::VARCHAR,
    $1:country::VARCHAR,
    $1:country_code::VARCHAR,
    $1:time_zone::VARCHAR,
    $1:payroll_iban::VARCHAR,
    $1:dept VARCHAR,
    $1:job_title::VARCHAR,
    $1:education::VARCHAR,
    $1:title::VARCHAR,
    $1:suffix::VARCHAR,
    metadata$filename::STRING,
    CURRENT_TIMESTAMP()
FROM @my_int_stage/employees.parque (file_format => parquetformat));

SELECT * FROM employees;


CREATE OR REPLACE VIEW employees_vw AS
(SELECT employee_id, dept, job_title
   FROM employees);


CREATE OR REPLACE STREAM employees_stream ON VIEW employees_vw;

select * from employees_vw;

UPDATE employees SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
UPDATE employees SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
UPDATE employees SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
UPDATE employees SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
UPDATE employees SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;

SELECT * FROM employees_stream;
