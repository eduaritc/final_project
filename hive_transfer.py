import os
import web_scrapping as ws
# from pyspark.python.pyspark.shell import spark
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession


URL_PRODUCT = "https://www.amazon.co.uk/Apple-iPhone-14-Plus-128/dp/B0BDJY2DFH/ref=sr_1_2_sspa?" \
              "keywords=iphone+14+pro+max&qid=1670164444&" \
              "sprefix=iphone+%2Caps%2C83&sr=8-2-spons&sp_csd=d2lkZ2V0TmFtZT1zcF9hdGY&psc=1"

def from_dict_to_spark_df(reviews_dict):
    """
    :param reviews_dict: python dictionary with Amazon's reviews of a specific apple's product
    :return: same information as input, but as a PySpark dataframe
    """
    df_reviews = spark.createDataFrame(data=reviews_dict)
    return df_reviews


soup = ws.get_the_soup(URL_PRODUCT)
dict_reviews = ws.get_product_reviews(soup)
reviews_df = from_dict_to_spark_df(dict_reviews)
# Create spark session with hive enabled
spark_session = SparkSession \
    .builder \
    .appName("PySpark_Hive_connection") \
    .getOrCreate()

reviews_df.createOrReplaceTempView("amazon_reviews")

# saving parquet CSV
spark_session.table("amazon_reviews").coalesce(1).write.mode("overwrite").option("header", "True").csv("amazon_reviews")
df_csv = spark.read.option("header", "true").csv("amazon_reviews")
os.system("streamlit run dashboard.py")
df_csv.show()

