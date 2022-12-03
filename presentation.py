from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


# connecting to the database
# .
host = "master1.internal.cloudapp.net"
# host = "4.227.242.50"
port = "8020"
# .config("spark.hadoop.fs.defaultFS", "hdfs://{}:{}/user/octoberbatch/group2/".format(host, port)) \
spark = SparkSession.\
         builder\
        .config("spark.jars", "C:\\Program Files (x86)\\MySQL\\Connector J 8.0\\mysql-connector-j-8.0.31.jar") \
        .master("local") \
        .appName("PySparkHiveConnection") \
        .enableHiveSupport() \
        .getOrCreate()

# loading the table customer info on the DF customersDF
customersDF = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/classicmodels") \
    .option("driver", "com.mysql.jdbc.Driver")\
    .option("dbtable", "customers") \
    .option("user", "root")\
    .option("password", "root").load()

# operation  and sending the result to hdfs
customersDF.filter(customersDF["creditLimit"] > 1000.00) \
    .groupBy("country") \
    .count() \
    .createOrReplaceTempView("custcreditover1k")
custcredover1k = spark.sql("select * from custcreditover1k")
spark.table("custcreditover1k").write.saveAsTable("custcreditover1k")
query = spark.sql("select * from custcreditover1k")
query.show()

# custcredover1k.write.mode("append").saveAsTable("custcreditover1k.table")

    # .coalesce(1) \
    # .write \
    # .mode("overwrite") \
    # .option("header", "true") \
    # .format("parquet") \
    # .save("customerscreditover1k.parquet")



