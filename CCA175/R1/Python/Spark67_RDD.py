from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

lines = sc.parallelize(['Its fun to have fun,','but you have to know how.'])
print(lines.collect())
r1 = lines.map(lambda line:line.replace(',','').replace('.','').lower())
r2 = r1.flatMap(lambda line:line.split(" "))
r3 = r2.map(lambda word:(word,1))
r4 = r3.reduceByKey(lambda x,y:x+y)
r5 = r4.map(lambda (word,num):(num,word))
r6 = r5.sortByKey(False)

print(r6.collect())