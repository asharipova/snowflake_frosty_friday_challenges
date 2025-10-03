/*Frosty Friday Consultants has been hired by the University of Frost’s history department; they want data on monarchs in their data warehouse for analysis. Your job is to take the JSON file located here (https://frostyfridaychallenges.s3.eu-west-1.amazonaws.com/challenge_4/Spanish_Monarchs.json), ingest it into the data warehouse, and parse it into a table that looks like this:

End Result:


If you can’t read the above right-click and view image in another tab.

Separate columns for nicknames and consorts 1 – 3, many will be null.
An ID in chronological order (birth).
An Inter-House ID in order as they appear in the file.
There should be 26 rows at the end.
Hints:

Make sure you don’t lose any rows along the way.
Be sure to investigate all the outputs and parameters available when transforming JSON.*/

USE DATABASE FROSTY_DB;
USE SCHEMA FRIDAY;
USE WAREHOUSE COMPUTE_WH;

CREATE OR ALTER STAGE my_int_stage;
DROP STAGE my_int_stage;
PUT file://c:\temp\data\Spanish_Monarchs.json @my_int_stage;

LIST @my_int_stage;

SHOW STAGES;

CREATE OR REPLACE FILE FORMAT jsonformat TYPE="JSON" COMPRESSION = GZIP;

CREATE OR REPLACE TABLE monarchs AS
(
SELECT 
    row_number() over (order by TO_DATE(monarchs.value:Birth::STRING, 'YYYY-MM-DD')) as id,
    monarchs.index+1 as inter_house_id,
    --src.$1 as data,
    era.value:Era::VARCHAR as era,
    houses.value:House::VARCHAR as house,
    monarchs.value:Name::VARCHAR as name,
    monarchs.value:Nickname[0]::VARCHAR as nickname_1,
    monarchs.value:Nickname[1]::VARCHAR as nickname_2,
    monarchs.value:Nickname[2]::VARCHAR as nickname_3,
    TO_DATE(monarchs.value:Birth::STRING, 'YYYY-MM-DD') as birth,
    monarchs.value:"Place of Birth"::VARCHAR as place_of_birth,
    TO_DATE(monarchs.value:"Start of Reign"::STRING, 'YYYY-MM-DD') as start_of_reign,
    monarchs.value:"Consort\/Queen Consort"[0]::VARCHAR as consort_or_queen_consort_1,
    monarchs.value:"Consort\/Queen Consort"[1]::VARCHAR as consort_or_queen_consort_2,
    monarchs.value:"Consort\/Queen Consort"[2]::VARCHAR as consort_or_queen_consort_3,
    TO_DATE(monarchs.value:"End of Reign"::STRING, 'YYYY-MM-DD') as end_of_reign,
    monarchs.value:Duration::STRING as duration,
    TO_DATE(monarchs.value:Death::STRING, 'YYYY-MM-DD') as death,
    SPLIT(monarchs.value:"Age at Time of Death"::VARCHAR, ' ')[0]::NUMBER as age_at_time_of_death_years,
    monarchs.value:"Place of Death"::VARCHAR as place_of_death,
    monarchs.value:"Burial Place"::VARCHAR as burial_place,
    --metadata$filename::STRING,
    CURRENT_TIMESTAMP() as ingestion_timestamp
FROM @my_int_stage/Spanish_Monarchs.json (file_format => jsonformat) src,
LATERAL FLATTEN(INPUT => src.$1) era,
LATERAL FLATTEN(INPUT => era.value:Houses) houses,
LATERAL FLATTEN(INPUT => houses.value:Monarchs) monarchs);

select * from monarchs;


