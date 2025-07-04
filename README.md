# FLQ Take Home Assignment
Take away home assignment for Senior Staff Data Engineer Position. 

## Background & Objective

This project simulates a real-world scenario for an accounting SaaS platform that helps automate and track the month-end close process, journal entries, and account reconciliations. MongoDB is used as the primary source for accounting records, workflow tasks, and audit trails, which must be transformed for analytics.

 The goal  is to build end to end pipleine with following
 - ingest data from MongoDB
 - Transform, flatten and calculate rollup for analytical use
 - Write into S3 in Apache Iceberg format
 - Implement iceberg optimization , adding partition, compaction, and snapshot retention policy.
 - Recommend open query engine 
 - Include unit test stubs
 - Provide alternative approach using Snowflake Variant for Json flattening and query iceberg tables.

## Assumption:

   - Deliverable will be shared as public gitrepo with sample JSONs, pyspark code , ddl example, unit tests and a README.
   - Since thre is no local enviornment, the code will be well-sytructured and comment but not fully runable end to end.
   - For CDC, entry_id plus a timetamp will be used to track incremental updates
   - Simple unit tests stubs will be included to show how the extract and transform steps could be validated.
   - In addition to the PySpark approach,  alternative design using Snowflake VARIANT for JSON flattening and querying Iceberg tables from Snowflake will also be
included.
  
## Project Structure (high level)
    project-root
    SourceData/
       --accounts.json
       --journal_entries.json
       --close_tasks.json
    Scripts/
       --write_collection_mongodb.py  (additonal scripts for write data into  local Mongodb)
       --extract_collection.py
       --transform.py
       --iceberg_optimization.sql
       --alternative_approach_snowflake.sql
    tests/
       --test_transform.py
    README.md  

## Approach
    **extract**
     - uses pyspark to extract json data from MongoDB Collenction into dataframe
    **transform**
     - flatten nested (journal_entries.lines) in separate rows
     - Calculates total debit and credit per journal entry.
     - create partition column year  and month from entry_date
    **Load**
     - Write to S3 in Iceberg format, partition by year and month
     - CDC update using Merge Into 
     
## Iceberg Optimization
   - partitioniong - Table partitioned by year/month to enable pruning and efficient scans. Example code included in 'transform.py'
   - compaction - Example 'call ' command to merge small files and optimze read perfoamnce
   - retention - Example code included in transform.py to shown retention policy manage storage cost.


## Querty Enginee recommendation 
   - **Trino**: Recommended as the primary open source query engine for icebergs table. Trino is highly performant for  ad-hoc querying and interactive analytics , uses distributed SQL, and supports complex
                queries directly on  lakehouse. Good choice when need speed , interactive queries across different data sources, cost, pluggables catalogs and flexibility without vendor lock-in.
   - **Snowflake**:If existing infrastructure on the Snowflake platform, this offer seamless integration, provide governance and cost effective for external tables. 
     
## Alternative design (snowflake)
  - Raw JSON in s3 and staged into a snowflake VARIANT table
  - Flattening using 'LATERAL FLATTEN' SQL in Snowflake
  - Transformed data is written back to S3 in Parquet format and registeed as an external Iceberg table for cross-engine querying.

## Testing
   - Basic unit test stubs included (PyTest) for verifying that the  transform steps run as expected.
   - test cover JSON flattening, schema verification.

## Improvment 
   - Add CI/CD workflow using Github action 
   - linting for code quality
   - better error handling with try/catch and logging
## Reference: 
   Example Mongo aggregation logic
    https://www.practical-mongodb-aggregations.com/examples/foundational/unpack-array-group-differently.html   

## Notes 
  - Local MongoDB and Spark were installed to run spot checkes and validate extract and transform scripts
  - Additional write step('write_collection_mongodb.py) included to load sample JSON.
 
    
    
