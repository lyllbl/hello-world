from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

initialScores = [("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma",98.0)]

#following is 2 ways to create dateframe, another is using schema = StructType
#ScoresDF = spark.createDataFrame(initialScores,['name','score'])

ScoresDF = spark.createDataFrame(initialScores,"name:string,score:float")

print(ScoresDF.show())

ScoresDF.registerTempTable("Scores")

resultDF = spark.sql("select name,avg(score) as avgScore from Scores group by name")

print(resultDF.show())