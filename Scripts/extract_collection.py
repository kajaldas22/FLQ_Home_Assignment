'''
extact_collection.py 

Extract Collection from MongoDB using pymongo
Load into Dataframe for transfor
'''
 
from pymongo import MongoClient
from pyspark.sql import SparkSession

def main():

    # Start Spart session
    spark=SparkSession.builder\
        .appName("ExtractFromMongoDB") \
        .getOrCreate()


    print("SparkSession tupe", type(spark))

    #Connect to local MongoDB with Pymongo ( note: I have installed mongoDB locally for run and spot check)

    client=MongoClient("mongodb://localhost:27017/")
    db= client["FLQAccounts"]


    # Extract collections: account, journal_entries, close_tasks

    # Extract account collection and clean '_id' field
    account_docs=[]
    for doc in  db["account"].find({}):
        doc.pop('_id',None)
        account_docs.append(doc)

    # Extract journal_entries and remove '_id' field
    journal_entries_docs=[]
    for doc in db["journal_entries"].find({}):
        doc.pop('_id',None)
        journal_entries_docs.append(doc)    
    

    close_tasks_docs=[]
    for doc in db["close_tasks"].find({}):
        doc.pop('_id',None)
        close_tasks_docs.append(doc)    
    
    
    #Load into Spark DataFrames 
    df_account=spark.createDataFrame(account_docs)
    df_close_tasks=spark.createDataFrame(close_tasks_docs)

    #Load  nested collection with Json reader 
    df_journal_entries=spark.read.json(spark.sparkContext.parallelize(journal_entries_docs))

    #Validate
    print("Account collection")
    df_account.show()

    print("Close Tasks ")
    df_close_tasks.show()

    print("Journal Enteries")
    df_journal_entries.show()
    
if __name__ == "__main__":
     main()

 