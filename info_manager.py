import os
import web_scrapping as ws
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '-- packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

DASHBOARD = "dashboard.py"
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
reviews_df.createOrReplaceTempView("amazon_reviews")

conf = SparkConf().set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true"). \
 set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true"). \
 setAppName("pyspark_aws").setMaster("local[*]")

sc=SparkContext.getOrCreate(conf)
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
accessKeyId="AKIAZKDJX3WCJE4R4JTN"
secretAccessKey="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"

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", accessKeyId)
hadoopConf.set("fs.s3a.secret.key", secretAccessKey)
hadoopConf.set("fs.s3a.endpoint", "s3-eu-west-2.amazonaws.com")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoopConf.set("fs.s3a.multiobjectdelete.enable", "false")

spark=SparkSession(sc)
reviews_df.coalesce(1).write.format('csv').option('header','true').save('s3a://finalprojectitc/amazon_reviews',mode='overwrite')
# reviews_df.write.format('csv').option('header','true').save('s3a://finalprojectitc/amazon_reviews/',mode='overwrite')



# spark_session = SparkSession \
#     .builder \
#     .appName("finalproject") \
#     .getOrCreate()

# reviews_df.createOrReplaceTempView("amazon_reviews")
# spark_session.table("amazon_reviews").coalesce(1).write.mode("overwrite").option("header", "True").csv("amazon_reviews")
# df_csv = spark.read.option("header", "true").csv("amazon_reviews")
# df_csv.show()
# os.system("streamlit run {}".format(DASHBOARD))



