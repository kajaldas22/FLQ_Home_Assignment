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
       --ingest.py
       --transform.py
       --iceberg_table.ddl
    tests/
       --test_etl_pipeline.py
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
   - partitioniong - include ddl 
   - compaction - example 'call 'data file'
   - retention - example
## Querty Enginee recommendation 
   - open source Trino,
   - Spark SQL
   - Snowflake : Iceberg as external table
## Alternative design (snowflake)

## Testing

## Improvment 
   - git action
   - 
## Summary
 
    
    
