from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

feedbackColumnList = [StructField("Name",StringType()),StructField("Date",StringType()),StructField("Num",StringType()),]

feedbackSchema = StructType(feedbackColumnList)

feedbackDF = spark.read.csv("hdfs://master:9000/sparkData/input/feedback.txt",schema = feedbackSchema, sep = '|')

vaildateT1 = "(\d+)\s(\w{3})(,)\s(\d{4})"
vaildateT2 = "(\d+)(/)(\d+)(/)(\d{4})"
vaildateT3 = "(\d+)(-)(\d+)(-)(\d{4})"
vaildateT4 = "(\w{3})\s(\d+)(,)\s(\d{4})"

vaildate = "(\d+)\s(\w{3})(,)\s(\d{4})|(\d+)(/)(\d+)(/)(\d{4})|(\d+)(-)(\d+)(-)(\d{4})|(\w{3})\s(\d+)(,)\s(\d{4})"

feedbackGoodRegDF = feedbackDF.select("Name",regexp_extract("Date",vaildate,0).alias("Date"),"Num")

feedbackGoodDF = feedbackGoodRegDF.where("Date is not null and length(Date)>0")

feedbackBadDF = feedbackDF.subtract(feedbackGoodDF)

feedbackGoodDF.write.csv("hdfs://master:9000/sparkData/output/37/DataFrame/Good",sep = "|")
feedbackBadDF.write.csv("hdfs://master:9000/sparkData/output/37/DataFrame/Bad",sep = "|")