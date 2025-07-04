'''
write collection mongodb
This script writes data from JSON files to MongoDB collections.
'''

from pyspark.sql import SparkSession

# Start Spark Session with Mongodb Spark connector

spark=SparkSession.builder \
    .appName("MongoDBWrite")    \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .getOrCreate()


# Show Spark version for 
print("spark version", spark.version)

# Read JSON files from SourceData folder

account_df= spark.read.json("SourceData/accounts.json", multiLine=True)
journal_entries_df=spark.read.json("SourceData/journal_entries.json",multiLine=True)
close_tasks_df=spark.read.json("SourceData/close_tasks.json", multiLine=True)



# show a few rows to check (#validation of load)
 
# account_df.show()
# journal_entries_df.show()
# close_tasks_df.show()


# Write DataFrame to MongoDB collections in 'FLQAccounts' databae  (collections: account, journal_entries, close_tasks)

account_df.write.format("mongodb") \
    .option("uri", "mongodb://127.0.0.1") \
    .option( "database","FLQAccounts") \
    .option("collection","account") \
    .mode("append") \
    .save()

journal_entries_df.write.format("mongodb") \
    .option("uri", "mongodb://127.0.0.1") \
    .option( "database","FLQAccounts") \
    .option("collection","journal_entries") \
    .mode("append") \
    .save()

close_tasks_df.write.format("mongodb") \
    .option("uri", "mongodb://127.0.0.1") \
    .option( "database","FLQAccounts") \
    .option("collection","close_tasks") \
    .mode("append") \
    .save()