from pyspark.sql import SparkSession
from operator import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")
orders = spark.read.jdbc("jdbc:mysql://localhost:3306/retail_db",table = 'orders',properties = {'user':'retail_dba','password':'cloudera'})
print(orders.show())

#DataFrame.rdd is attribute, not funcation
r1 = orders.rdd
print(r1.take(5))
#notice datatime is not format, it is datetime.datetime(2013, 7, 25, 0, 0)
r2 = r1.map(lambda row:(row.order_status,tuple(row)))
print(r2.take(5))

#countByKey return is not RDD, it is defaultdict
print("countByKey")
r3 = r2.countByKey()
print(r3)

#groupByKey should use for loop to count
print("groupByKey")
r4 = r2.groupByKey()
for (key,valueList) in r4.collect():
	count = 0
	for value in valueList:
		count = count + 1
	print(key,count)


print("reduceByKey")
r5 = r2.map(lambda (k,v):(k,1)).reduceByKey(add)
print(r5.collect())

#aggregateByKey's zeroValue could be int
print("aggregateByKey")
zeroValue = 0
def seqFunc(x,y):
	return x + y

def combFunc(x,y):
	return x + y
r6 = r2.map(lambda (k,v):(k,1)).aggregateByKey(zeroValue, seqFunc, combFunc)
print(r6.collect())

#combineByKey's createCombiner should be funcation and return not be 0
print("combineByKey")
def createCombiner(x):
	return 1
def mergeValue(x,y):
	return x + y
def mergeCombiners(x,y):
	return x + y
r7 = r2.map(lambda (k,v):(k,1)).combineByKey(createCombiner, mergeValue, mergeCombiners)
print(r7.collect())


