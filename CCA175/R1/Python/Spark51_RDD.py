from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize([1,2,1,3],1)
print (a.collect())

b = a.map(lambda item:(item,"b"))

print (b.collect())

c = a.map(lambda item:(item,"c"))

print (c.collect())

#groupWith Alias for cogroup but with support for multiple RDDs.
group = b.groupWith(c)

for x,y in group.collect():
	print(x,tuple(map(list,y)))

print("new 1 way")
for x,y in group.collect():
	print(x,map(list,y))




x = sc.parallelize([("a", 1), ("b", 4)])
y = sc.parallelize([("a", 2)])
#[print((x, tuple(map(list, y)))) for x, y in sorted(list(x.cogroup(y).collect()))]
#[('a', ([1], [2])), ('b', ([4], []))]

for x,y in sorted(list(x.cogroup(y).collect())):
	print(x,y)

	
	
for (a,b) in group.collect():
   print "---" ,a
   for (c) in b:
      print "***",c
      for d in c:
         print d
