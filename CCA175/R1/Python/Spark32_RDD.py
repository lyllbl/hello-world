from pyspark.sql import SparkSession
 


spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

FileRDD = sc.textFile("hdfs://master:9000/sparkData/input/file[1-3].txt")

print(FileRDD.collect())

FileFlatRDD = FileRDD.flatMap(lambda words:words.split(' '))

print(FileFlatRDD.collect())

FilterWordList = ("a","the","an", "as", "a","with","this","these","is","are","in", "for", "to","and","The","of")

print(FilterWordList)

FileFlatFilterRDD = FileFlatRDD.filter(lambda word: word not in FilterWordList)

print(FileFlatFilterRDD.collect())

FileFlatFilterCountRDD = FileFlatFilterRDD.map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)

print(FileFlatFilterCountRDD.collect())

#FileFlatFilterCountReverseRDD = FileFlatFilterCountRDD.map(lambda wordnum:(wordnum[1],wordnum[0]))

FileFlatFilterCountReverseRDD = FileFlatFilterCountRDD.map(lambda (word,num):(num,word))

print(FileFlatFilterCountReverseRDD.collect())

FileFlatFilterCountReverseSortRDD = FileFlatFilterCountReverseRDD.sortByKey(False)

print(FileFlatFilterCountReverseSortRDD.collect()) 

FileFlatFilterCountReverseSortRDD.saveAsTextFile("hdfs://master:9000/sparkData/output/32/RDD","org.apache.hadoop.io.compress.GzipCodec")

