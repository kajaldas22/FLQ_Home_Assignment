/*
 alternative approach to flatten json  in snowflake and then write it into Iceberg table
 assumption- raw JSON in s3 bucket
  
*/

--create staging in snowflake and attached to S3 

CREATE OR REPLEACE my_s3_stage 
    URL= 's3://mongodb-json-exports/'
    

 -- create table with VARIANT data type to store json value

CREATE OR REPLEACE table raw_db.mongo_journal_entries_stage(data variant)

-- 
COPY INTO @raw_db.mongo_journal_entries_stage
FROM '@my_s3_stage'
FILE_FORMAT= (TYPE=JSON)

--Flatten journal_entries.lines into separate rows 
--Add partition column year and month , write into S3 as Parquet 

COPY INTO @my_s3_stage/journal_entries_curated/
FROM (
  SELECT 
    data:entry_id::string as entry_id,
    data:date::Date as entry_Date,
    l.value:account_id::string as account_id,
    l.value:debit::number as debit,
    l.value:credit::number as credit,
    YEAR( data:date::Date) as year,
    month(date:date::Date)as month
from raw_db.mongo_journal_entries_stage ,
    LATERAL FLATTEN( input=> data:lines)
)
FILE_FORMAT = (TYPE = PARQUET);

--Register the Parquet files as an Iceberg extenral table 
--So this queryable by Snowflake and  query engine(trino,spark,athena )
CREATE EXTERNAL TABLE analytics_db.journal_entries_iceberg
  LOCATION = '@my_s3_stage/journal_entries_curated/'
  FILE_FORMAT = (TYPE = PARQUET)
  AUTO_REFRESH = TRUE
  TABLE_FORMAT = ICEBERG
  PARTITION BY (year, month);

