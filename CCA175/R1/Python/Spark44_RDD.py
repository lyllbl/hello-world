from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("Error")

file1RDD =  sc.textFile("hdfs://master:9000/sparkData/input/file1.txt")
file2RDD =  sc.textFile("hdfs://master:9000/sparkData/input/file2.txt")
file3RDD =  sc.textFile("hdfs://master:9000/sparkData/input/file3.txt")
file4RDD =  sc.textFile("hdfs://master:9000/sparkData/input/file4.txt")

file1DataRDD = file1RDD.map(lambda line:line.replace('.','').lower()).flatMap(lambda line:line.split(' ')).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y).map(lambda (x,y):(y,x)).sortByKey(False)
file2DataRDD = file2RDD.map(lambda line:line.replace('.','').lower()).flatMap(lambda line:line.split(' ')).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y).map(lambda (x,y):(y,x)).sortByKey(False)
file3DataRDD = file3RDD.map(lambda line:line.replace('.','').lower()).flatMap(lambda line:line.split(' ')).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y).map(lambda (x,y):(y,x)).sortByKey(False)
file4DataRDD = file4RDD.map(lambda line:line.replace('.','').lower()).flatMap(lambda line:line.split(' ')).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y).map(lambda (x,y):(y,x)).sortByKey(False)

file1MaxRDD = file1DataRDD.keys().max()
file2MaxRDD = file2DataRDD.keys().max()
file3MaxRDD = file3DataRDD.keys().max()
file4MaxRDD = file4DataRDD.keys().max()

file1Name = file1RDD.name()
file2Name = file2RDD.name()
file3Name = file3RDD.name()
file4Name = file4RDD.name()

file1MaxValueRDD = file1DataRDD.filter(lambda (num,word): num == file1MaxRDD).map(lambda (num,word):(word,num)).collect()
file2MaxValueRDD = file2DataRDD.filter(lambda (num,word): num == file2MaxRDD).map(lambda (num,word):(word,num)).collect()
file3MaxValueRDD = file3DataRDD.filter(lambda (num,word): num == file3MaxRDD).map(lambda (num,word):(word,num)).collect()
file4MaxValueRDD = file4DataRDD.filter(lambda (num,word): num == file4MaxRDD).map(lambda (num,word):(word,num)).collect()

file1Result = file1Name+"->"+str(file1MaxValueRDD)
file2Result = file2Name+"->"+str(file2MaxValueRDD)
file3Result = file3Name+"->"+str(file3MaxValueRDD)
file4Result = file4Name+"->"+str(file4MaxValueRDD)

file1ResultRDD = sc.parallelize([file1Result])
file2ResultRDD = sc.parallelize([file2Result])
file3ResultRDD = sc.parallelize([file3Result])
file4ResultRDD = sc.parallelize([file4Result])

fileResultRDD = file1ResultRDD.union(file2ResultRDD).union(file3ResultRDD).union(file4ResultRDD)

print(fileResultRDD.collect())

fileResultRDD.saveAsTextFile("hdfs://master:9000/sparkData/output/44/RDD")
