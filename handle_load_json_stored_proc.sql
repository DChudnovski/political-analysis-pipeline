-- SQL for creating a stored procedure that will be called by the DAG
CREATE OR REPLACE PROCEDURE handle_load_json() 
    RETURNS varchar
    LANGUAGE SQL
    AS
    $$
    BEGIN
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
    CREATE OR REPLACE VIEW analytics.new_york_city_mayor_race 
    AS
    SELECT 
            contractid, 
            marketid, 
            name, 
            ZEROIFNULL(lasttradeprice) AS lasttradeprice, 
            ZEROIFNULL(bestbuyyescost) as bestbuyyescost, 
            ZEROIFNULL(bestbuynocost) AS bestbuynocost, 
            ZEROIFNULL(bestsellyescost) AS bestsellyescost, 
            ZEROIFNULL(bestsellnocost) AS bestsellnocost,
            ZEROIFNULL(lastcloseprice) AS lastcloseprice, 
            timestamp
    FROM public.political_betting_contracts
    WHERE marketid = 8095;
 
    END;
    $$;