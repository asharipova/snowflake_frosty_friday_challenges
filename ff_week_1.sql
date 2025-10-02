/*
FrostyFriday Inc., your benevolent employer, has an S3 bucket that is filled with .csv data dumps. This data is needed for analysis. 
Your task is to create an external stage, and load the csv files directly from that stage into a table.

The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_1/
*/

CREATE DATABASE IF NOT EXISTS FROSTY_DB;
CREATE SCHEMA IF NOT EXISTS FROSTY_DB.FRIDAY;

USE DATABASE FROSTY_DB;
USE SCHEMA FRIDAY;
USE WAREHOUSE COMPUTE_WH;


CREATE OR REPLACE STAGE frosty_stage 
URL = "s3://frostyfridaychallenges/challenge_1/";

LS @frosty_stage;
SELECT t.$1, t.$2, t.$3 FROM @frosty_stage/3.csv t;

CREATE OR REPLACE FILE FORMAT csvformat TYPE = 'CSV' SKIP_HEADER = 1;

SELECT
    $1 AS data,
    metadata$filename AS file_name,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM
    @frosty_stage (FILE_FORMAT => 'csvformat');

CREATE TABLE frosty_tbl (data VARCHAR,
                         file_name VARCHAR(200),
                         load_ts TIMESTAMP);


COPY INTO frosty_tbl FROM (
SELECT
    $1 AS data,
    metadata$filename AS file_name,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM
    @frosty_stage) FILE_FORMAT = (FORMAT_NAME = 'csvformat');

SELECT * FROM TABLE(VALIDATE(friday.frosty_tbl, JOB_ID => '_last'));
SELECT * FROM TABLE(VALIDATE(friday.frosty_tbl, JOB_ID => '01bf6bbd-0003-9755-0000-0009d23627c1')); 


SELECT * FROM frosty_tbl;

