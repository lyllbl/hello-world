from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

file1 = sc.textFile("hdfs://master:9000/sparkData/input/spark75file1.txt")
print(file1.collect())

r1 = file1.glom().map(lambda paragraph:"".join(paragraph)).flatMap(lambda paragraph:paragraph.split('.'))
print(r1.collect())
r2 = r1.map(lambda sentence:sentence.split(" "))
print(r2.collect())
r3 = r2.flatMap(lambda sentence:[((sentence[i],sentence[i+1]),1) for i in range(0,len(sentence)-1)])
print(r3.collect())
r4 = r3.filter(lambda (k,v):k[0] == k[1])
print(r4.collect())
r5 = r4.reduceByKey(lambda x,y:x+y).map(lambda word:(word[1],word[0])).sortByKey(False)
print(r5.collect())