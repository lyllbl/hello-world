from pyspark.sql import SparkSession

from operator import *


spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

orderitems = sc.textFile("hdfs://master:9000/user/root/p89_order_items")
print(orderitems.take(5))
r1 = orderitems.map(lambda record:float(record.split(',')[4]))
print(r1.take(5))
#need to import operator lib
r2 = r1.reduce(add)
print(r2)

#max and min is python Built-in Functions
r3 = r1.reduce(max)
print(r3)
r4 = r1.reduce(min)
print(r4)

# 'float' object is not iterable
r6 = r1.reduce(sum)
print(r6)
r5 = r1.reduce(average)
print(r5)