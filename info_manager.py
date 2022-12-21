import web_scrapping as ws
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
import socket
import pickle
import pandas as pd


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


def server_program(reviews):
     # get the hostname
    host = socket.gethostname()
    port = 5000  # initiate port above 1024

    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(1)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    while True:
        # receive data stream. it won't accept data packet greater than 1024 bytes
        received_data = conn.recv(1024).decode()
        if not received_data:
            # if data is not received break
            break
        print("from connected user: " + str(received_data))
        df_reviews = pd.DataFrame(reviews.head(n=int(received_data)))
        print(df_reviews)
        sent_data = pickle.dumps(df_reviews)
        conn.send(sent_data)
    conn.close()  # close the connection



soup = ws.get_the_soup(URL_PRODUCT)
dict_reviews = ws.get_product_reviews(soup)
reviews_df = from_dict_to_spark_df(dict_reviews)

spark_session = SparkSession \
    .builder \
    .appName("finalproject") \
    .getOrCreate()

reviews_df.createOrReplaceTempView("amazon_reviews")
spark_session.table("amazon_reviews").coalesce(1).write.mode("overwrite").option("header", "True").csv("amazon_reviews")
df_csv = spark.read.option("header", "true").csv("amazon_reviews")
server_program(df_csv.toPandas())
# os.system("streamlit run {}".format(DASHBOARD))


