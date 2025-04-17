CREATE DATABASE IF NOT EXISTS political_analysis;
USE DATABASE political_analysis;

CREATE SCHEMA IF NOT EXISTS analytics;

USE SCHEMA public;

CREATE TABLE IF NOT EXISTS raw_json (
        src variant
);

CREATE TABLE IF NOT EXISTS market_json 
(
        marketid number,
        shortname varchar(100),
        src variant,
        timestamp datetime
);

CREATE TABLE IF NOT EXISTS political_betting_contracts 
( 
        contractid number,
        marketid number,
        marketname string ,
        name string,
        lasttradeprice float,
        bestbuyyescost float,
        bestbuynocost float,
        bestsellyescost float,
        bestsellnocost float,
        lastcloseprice float,
        timestamp datetime
        
);

TRUNCATE TABLE raw_json;

COPY INTO raw_json FROM @jsondata/load_json.json.gz
FILE_FORMAT = (TYPE = JSON);

TRUNCATE TABLE market_json;
INSERT INTO market_json 
SELECT 
        value:id::number AS marketid, 
        value:shortName::varchar(100) AS shortname, 
        VALUE AS src,  
        value:timeStamp::datetime AS timestamp, 
FROM raw_json, LATERAL FLATTEN ( INPUT => SRC:markets );


INSERT INTO political_betting_contracts 
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
        value:lastClosePrice::float AS lastcloseprice,
        timestamp::datetime AS timestamp
 FROM market_json, LATERAL FLATTEN ( INPUT => SRC:contracts );

--SELECT * FROM political_betting_contracts;

--TRUNCATE TABLE political_betting_contracts;

--SELECT * FROM political_analysis.public.raw_json;

--SELECT * FROM market_json;

--DROP TABLE raw_json;
--DROP TABLE market_json;
--DROP TABLE political_betting_contracts;

CREATE OR REPLACE VIEW analytics.new_york_city_mayor_race 
AS
SELECT contractid, marketid, name, lasttradeprice, bestbuyyescost, bestbuynocost, bestsellyescost, bestsellnocost,lastcloseprice, timestamp
FROM public.political_betting_contracts
WHERE marketid = 8095;

SELECT * FROM analytics.new_york_city_mayor_race;