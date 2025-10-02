/*In Week 1 we looked at ingesting S3 data, now it’s time to take that a step further. So this week we’ve got a short list of tasks for you all to do.

The basics aren’t earth-shattering but might cause you to scratch your head a bit once you start building the solution.

Frosty Friday Inc., your benevolent employer, has an S3 bucket that was filled with .csv data dumps. These dumps aren’t very complicated and all have the same style and contents. All of these files should be placed into a single table.

However, it might occur that some important data is uploaded as well, these files have a different naming scheme and need to be tracked. We need to have the metadata stored for reference in a separate table. You can recognize these files because of a file inside of the S3 bucket. This file, keywords.csv, contains all of the keywords that mark a file as important.

Objective:

Create a table that lists all the files in our stage that contain any of the keywords in the keywords.csv file.

The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_3/*/

USE DATABASE FROSTY_DB;
USE SCHEMA FRIDAY;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE STAGE ff_week3_stage URL = "s3://frostyfridaychallenges/challenge_3/";

LIST @ff_week3_stage;

select $1 from @ff_week3_stage/keywords.csv;

/*keyword
stacy_forgot_to_upload
extra
added*/

SELECT METADATA$FILENAME AS file_name,
    COUNT(METADATA$FILE_ROW_NUMBER) AS number_of_rows,
    METADATA$FILE_LAST_MODIFIED AS FILE_LAST_MODIFIED
FROM @ff_week3_stage(pattern=>'.*(stacy_forgot_to_upload|extra|added).*') t
GROUP BY METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED;

CREATE OR REPLACE FILE FORMAT csvformat TYPE = 'CSV' SKIP_HEADER = 1;

CREATE TABLE stage_metadata (stage_name VARCHAR, file_name VARCHAR, number_of_rows NUMBER, FILE_LAST_MODIFIED TIMESTAMP)
AS (
SELECT 'ff_week3_stage', t.METADATA$FILENAME AS file_name,
    COUNT(t.METADATA$FILE_ROW_NUMBER) AS number_of_rows,
    t.METADATA$FILE_LAST_MODIFIED AS FILE_LAST_MODIFIED
FROM @ff_week3_stage/keywords.csv (file_format => 'csvformat') f, @ff_week3_stage t
WHERE CONTAINS(t.METADATA$FILENAME, f.$1)
GROUP BY t.METADATA$FILENAME, t.METADATA$FILE_LAST_MODIFIED);


SELECT * FROM stage_metadata;
