from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

keysWithValuesList = ["foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D"]

print(keysWithValuesList)

data = sc.parallelize(keysWithValuesList)
print(data.first())
print(data.collect())

kv = data.map(lambda items:items.split('=')).map(lambda items:(items[0],items[1]))

print(kv.first())
print(kv.collect())

initialCount = 0
addToCounts = (lambda x,y : x + 1)
sumPartitionCounts = (lambda x,y : x + y)

countByKey = kv.aggregateByKey(initialCount,addToCounts,sumPartitionCounts)

print(countByKey.collect())

initialSet = set()
print(initialSet)


#addToSet = (lambda x,y : x.add(y))
#mergePartitionSets = (lambda x,y : x.update(y))



def addToSet(a,b):
	a.add(b)
	return a
#	return a.add(b)
	
def mergePartitionSets(a,b):
	a.update(b)
	return a
#	return a.update(b)

#using lambda will return None, just like function return expression.
#Because return expression just like invoke no-return function

uniqueByKey = kv.aggregateByKey(initialSet,addToSet,mergePartitionSets)

print(uniqueByKey.collect())

