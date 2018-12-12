from pyspark.sql import SparkSession
import re
import sys



spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

vaildateT1 = "(\d+)\s(\w{3})(,)\s(\d{4})"
vaildateT2 = "(\d+)(/)(\d+)(/)(\d{4})"
vaildateT3 = "(\d+)(-)(\d+)(-)(\d{4})"
vaildateT4 = "(\w{3})\s(\d+)(,)\s(\d{4})"

#PatternVaildateT1 = re.compile(vaildateT1)
#result = PatternVaildateT1.match("11 Jan, 2015")

#is equivalent to
#result = re.match(vaildateT1,"11 Jan, 2015")

feedbackRDD = sc.textFile("hdfs://master:9000/sparkData/input/feedback.txt")

#feedbackValidate1RDD = feedbackRDD.filter(lambda line:re.match(vaildateT1,line.split('|')[1]))
#feedbackValidate2RDD = feedbackRDD.filter(lambda line:re.match(vaildateT2,line.split('|')[1]))
#feedbackValidate3RDD = feedbackRDD.filter(lambda line:re.match(vaildateT3,line.split('|')[1]))
#feedbackValidate4RDD = feedbackRDD.filter(lambda line:re.match(vaildateT4,line.split('|')[1]))
#feedbackGoodRecord = feedbackValidate1RDD.union(feedbackValidate2RDD).union(feedbackValidate3RDD).union(feedbackValidate4RDD)


feedbackGoodRecord = feedbackRDD.filter(lambda line:re.match(vaildateT1,line.split('|')[1]) or re.match(vaildateT2,line.split('|')[1]) or re.match(vaildateT3,line.split('|')[1]) or re.match(vaildateT4,line.split('|')[1]))

feedbackBadRecord = feedbackRDD.subtract(feedbackGoodRecord)

print(feedbackGoodRecord.collect())
print(feedbackBadRecord.collect())

feedbackGoodRecord.saveAsTextFile("hdfs://master:9000/sparkData/output/37/RDD/GOOD")

feedbackBadRecord.saveAsTextFile("hdfs://master:9000/sparkData/output/37/RDD/Bad")

#subtract only use for set object not list,use map(lambda line:(line[0],line[1],line[2])) to change data type
#feedbackValidate1RDD = feedbackRDD.map(lambda line:line.split('|')).filter(lambda line:re.match(vaildateT1,line[1])).map(lambda line:(line[0],line[1],line[2]))
#ALLRDD = feedbackRDD.map(lambda line:line.split('|')).map(lambda line:(line[0],line[1],line[2]))
#subRDD = ALLRDD.subtract(feedbackValidate1RDD)
#print(subRDD.collect())

