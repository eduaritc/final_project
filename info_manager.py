import os
import pandas as pd


cf = "review_info"
hbase_table = {
    "table":{"namespace":"default", "name":"amazon_reviews"}, \
    }
columns = []

amazon_reviews_df = pd.read_csv("amazon_reviews.csv")

for index, row in amazon_reviews_df.iterrows():
    hbase_table[index] = {
                "col0":{"cf":cf, "colour":row["colour"]}, \
                "col1":{"cf":cf, "country":row["country"]}, \
                "col2":{"cf":cf, "date":row["date"]}, \
                "col3":{"cf":cf, "price":row["price"]}, \
                "col4":{"cf":cf, "size":row["size"]}, \
                "col5":{"cf":cf, "stars":row["stars"]}, \
                "col6":{"cf":cf, "text":row["text"]}, \
                "col7":{"cf":cf, "title":row["title"]} \
                }
# print(hbase_table)
amazon_reviews_pandas_df = pd.DataFrame(hbase_table)
amazon_reviews_pandas_df.to_csv("amazon_reviews_hbase.csv")
# print(hbase_table)



