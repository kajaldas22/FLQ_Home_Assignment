
'''
transform.py
Scropt transform collections dataframe by flattening , aggregating and adding partition
    - Flatten nested 'lines' into separate rows
    - Calculate total debit and credit
    - Add partiition (year and month from entry_date)
    - example write to iceberg
    - example Merget into CDC
'''

from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import col, explode, to_date,year, month,sum   


def main():

    #Start Spark session
    spark=SparkSession.builder \
        .appName("transformJournal") \
        .getOrCreate()

    #Connect to MongoDB
    client=MongoClient("mongodb://localhost:27017/")
    db= client["FLQAccounts"]
    
    #Extact journal entries for flattening

    journal_entries_docs=[]
    for doc in db["journal_entries"].find({}):
        doc.pop('_id',None)
        journal_entries_docs.append(doc)    

    df_journal_entries = spark.read.json(spark.sparkContext.parallelize(journal_entries_docs))

    print("Journal Entries DataFrame Schema:", df_journal_entries.schema)

    #flatten  'lines' into separate rows
    df_flatten = df_journal_entries.select("entry_id", "date",
                                           explode("lines").alias("line"),
                                           "description")

    final_df_flatten = df_flatten.select(
        "entry_id",
        col("date").alias("entry_date")  ,
        "line.account_id",
        "line.debit",
        "line.credit",
        "description"
    )

    #Add month and year partition from entry_date
    final_df_flatten=final_df_flatten.withColumn("year",year(to_date(col("entry_date")))) \
                                 .withColumn("month",month(to_date(col("entry_date"))))
    
#df_journal_entries.withColumn("line", explode(col("lines")))
    final_df_flatten.show()

    #totals per entry_id , year and month 
    final_df_total=final_df_flatten.groupBy("entry_id","year","month") \
        .agg(
                 sum("debit").alias("total_debit"),
                 sum("credit").alias("total_credit")
        )
    final_df_total.show()

    #  Example: Iceberg table write 
    # ******* Write transformed data into  Iceberg table (example) . In prod, this would write Parquet files to S3  *******


    # final_df_total.write.format("iceberg") \
    #     .mode("append") \
    #     .partitionBy("year", "month") \
    #     .save("iceberg_catalog.db.journal_entries")

    # ******* Merge into CDC (Change Data Capture) table example **********
    ''' 
        Merge  into iceberg_catalog.db.journal_entries AS target
        USING source_dataframe as source
        ON target.entry_id = source.entry_id
        and target.entry_date = source.entry_date
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
     '''
    
 
if __name__=="__main__":
    main()