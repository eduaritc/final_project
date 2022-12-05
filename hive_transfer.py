import web_scrapping as ws
from pyspark.conf import SparkConf
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, MapType


URL_PRODUCT = "https://www.amazon.co.uk/Apple-iPhone-14-Plus-128/dp/B0BDJY2DFH/ref=sr_1_2_sspa?" \
              "keywords=iphone+14+pro+max&qid=1670164444&" \
              "sprefix=iphone+%2Caps%2C83&sr=8-2-spons&sp_csd=d2lkZ2V0TmFtZT1zcF9hdGY&psc=1"

def from_dict_to_sparkdf(reviews_dict):
    """
    :param reviews_dict: python dictionary with Amazon's reviews of a specific apple's product
    :return: same information as input, but as a PySpark dataframe
    """
    df = spark.createDataFrame(data=reviews_dict)
    df.printSchema()
    df.show(truncate=False)

soup = ws.get_the_soup(URL_PRODUCT)
dict_reviews = ws.get_product_reviews(soup)
from_dict_to_sparkdf(dict_reviews)

