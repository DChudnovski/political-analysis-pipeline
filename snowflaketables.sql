--This SQL file contains the queries for loading the semi-structured JSON data into a structured table using Snowflakes 'FLATTEN' functions
USE DATABASE political_analysis;
USE SCHEMA public;
CREATE OR REPLACE TABLE raw_json(
    SRC VARIANT
);
COPY INTO political_analysis.public.raw_json FROM @--<stage and filename go here>
FILE_FORMAT = (TYPE = JSON);

SELECT * FROM political_analysis.public.json_test;

CREATE OR REPLACE TABLE market_json AS 
SELECT
        value:id::number AS marketid, 
        value:shortName::varchar(100) AS shortname,
        VALUE AS src  
FROM raw_json, LATERAL FLATTEN ( INPUT => SRC:markets );
CREATE OR REPLACE TABLE political_betting_contracts AS
SELECT 
        value:id::number AS contractid,
        marketid::number AS marketid,
        shortName::string AS marketname,
        value:shortName::string AS name,
        value:lastTradePrice::float AS lasttradeprice,
        value:bestBuyYesCost::float AS bestbuyyescost,
        value:bestBuyNoCost::float AS bestbuynocost,
        value:bestSellYesCost::float AS bestsellyescost,
        value:bestSellNoCost::float AS bestsellnocost,
        value:lastClosePrice::float AS lastcloseprice
 FROM market_json, LATERAL FLATTEN ( INPUT => SRC:contracts );