from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

z = sc.parallelize([1,2,3,4,5,6],2)

#if not use list(iterator), then that return memory address
def myFun(index,iterator):
	yield (index,list(iterator))


print(z.collect())

indexRDD = z.mapPartitionsWithIndex(myFun)
print(indexRDD.collect())

value = z.aggregate(5,lambda a,b:max(a,b),lambda a,b:a+b)

print(value)