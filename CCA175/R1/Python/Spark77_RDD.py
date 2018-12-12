from pyspark.sql import SparkSession
from operator import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

orders = sc.textFile("hdfs://master:9000/user/root/p89_orders")
orderitem = sc.textFile("hdfs://master:9000/user/root/p89_order_items")
print(orders.take(5))
print(orderitem.take(5))

o1 = orders.map(lambda record:record.split(",")).map(lambda cols:(cols[0],cols[1]))
i1 = orderitem.map(lambda record:record.split(",")).map(lambda cols:(cols[1],cols[4]))
print(o1.take(5))
print(i1.take(5))
joinoi1 = o1.join(i1)
print(joinoi1.take(5))
joinoi2 = joinoi1.map(lambda (k,v):((k,v[0]),float(v[1])))
print(joinoi2.take(5))

output1 = joinoi2.reduceByKey(add)
print(output1.take(5))

joinoi3 = joinoi1.map(lambda (k,v):(v[0],float(v[1])))
print(joinoi3.take(5))
print("combineByKey")
def createCombiner(rev):
	return (rev,1)

def mergeValue(x,y):
	return (x[0] + y,x[1] + 1)

def mergeCombiners(x,y):
	return (x[0] + y[0],x[1] + y[1])
output2 = joinoi3.combineByKey(createCombiner, mergeValue, mergeCombiners).map(lambda (k,v):(k,(v[0],(v[0]/v[1]))))
print(output2.take(5))

print("aggregateByKey")
zeroValue = (0,0)
def seqFunc(x,y):
	return (x[0] + y,x[1] + 1)
def combFunc(x,y):
	return (x[0] + y[0],x[1] + y[1])

output3 = joinoi3.aggregateByKey(zeroValue, seqFunc, combFunc).map(lambda (k,v):(k,(v[0],(v[0]/v[1]))))
print(output3.take(5))