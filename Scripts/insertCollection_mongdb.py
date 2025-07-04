# install pymongo  library 

from pymongo import MongoClient
import pandas as pd

client=MongoClient("mongodb://localhost:27017/")

# Switch to db created through command line(bash)
db= client["FLQAccounts"]
mcollection=db["account"]

#looping through the collection and display

# for doc in mcollection.find():
#     print(doc)

#cursor=client.list_databases()

# for db in cursor:
#     print("name of dbs:" ,db['name'])

docs= list(mcollection.find())

#convert it to dataframe

df=pd.DataFrame(docs)

print("dataframe\n", df)

