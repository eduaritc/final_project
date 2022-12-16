import os
import pandas as pd


cf = "review_info"
hbase_table = {
    "table":{"namespace":"default", "name":"amazon_reviews"}, \
    "rowkey":"key", \
    "columns": {}
    }
columns = []

amazon_reviews_df = pd.read_csv("amazon_reviews.csv")
for index, row in amazon_reviews_df.iterrows():
    columns.append({
        "rowkey":index, \
        "col"+str(index):{"cf":cf, "colour":row["colour"]}, \
        "col"+str(index):{"cf":cf, "country":row["country"]}, \
        "col"+str(index):{"cf":cf, "date":row["date"]}, \
        "col"+str(index):{"cf":cf, "price":row["price"]}, \
        "col"+str(index):{"cf":cf, "size":row["size"]}, \
        "col"+str(index):{"cf":cf, "stars":row["stars"]}, \
        "col"+str(index):{"cf":cf, "text":row["text"]}, \
        "col"+str(index):{"cf":cf, "title":row["title"]}
    })
hbase_table["columns"] = columns
amazon_reviews_pandas_df = pd.DataFrame(hbase_table)
amazon_reviews_df.to_csv("amazon_reviews_hbase.csv")
print(hbase_table)



