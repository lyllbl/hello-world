from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

products = sc.textFile("hdfs://master:9000/user/root/p93_products")
print(products.take(5))
r1 = products.map(lambda row:row.split(",")).map(lambda row:(float(row[4]) if row[4] <> '' else 0.0,row[0])).filter(lambda (k,v):k <> 0.0)
print(r1.take(5))

print("ascending")
r2 = r1.sortByKey(True)
print(r2.take(5))
print("descending")
r3 = r1.sortByKey(False)
print(r3.take(5))

#This method should only be used if the resulting array is expected to be small, as all the data is loaded into the drivers memory
#Get the top N elements from an RDD.
#It returns the list sorted in descending order.
print("top")
output1 = r1.top(10, key = lambda (k,v):(-k,v))
print(output1)

#This method should only be used if the resulting array is expected to be small, as all the data is loaded into the drivers memory
#Get the N elements from an RDD ordered in ascending order or as specified by the optional key function.
print("takeOrdered")
output1 = r1.takeOrdered(10, key = lambda (k,v):(-k,v))
print(output1)