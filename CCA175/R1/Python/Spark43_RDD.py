from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

#TypeError: int() argument must be a string or a number, not 'list'
#grouped = sc.parallelize((1,"two"),[(3,4),(5,6)])

grouped = sc.parallelize(((1,"two"),[(3,4),(5,6)]))
print(grouped.take(5))
print(grouped.keys().take(5))
print(grouped.first())
#DataRdd = grouped.map(lambda x: (2,x)).reduceByKey(lambda x,y: ([x,x],y)).values().flatMap(lambda x:x).map(lambda x: (3,x)).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).values().collect()

#a1 = grouped.map(lambda x: (2,x)).reduceByKey(lambda x,y: ([x,x],y)).values().first()
#print(a1)

DataRdd = grouped.map(lambda x: (2,x)).reduceByKey(lambda x,y: (x,y)).values().flatMapValues(lambda x:x).map(lambda (k,v):(k[0],k[1],v[0],v[1]))


DataRdd.saveAsTextFile("hdfs://master:9000/sparkData/output/43/RDD")



