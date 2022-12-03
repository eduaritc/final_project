# This is a sample Python script.
from pyspark import SparkContext

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    sc = SparkContext("local[*]", "pythonProjectPySpark")
    rdd1 = sc.textFile("sample.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd21 = rdd2.map(lambda x: x.lower())
    rdd3 = rdd21.map(lambda x: (x, 1))
    rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
    res = rdd4.collect()
    for item in res:
        print(item)
