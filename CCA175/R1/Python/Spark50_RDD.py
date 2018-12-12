from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

initialScores = [("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma",98.0)]

wilmaAndFredScores = sc.parallelize(initialScores)

print(wilmaAndFredScores.first())
print(wilmaAndFredScores.collect())

#createScoreCombiner must be start with 1
def createScoreCombiner(score):
	return (1,score)
	
def scoreCombiner(num,score):
	return (num[0]+1,num[1]+score)
	
def scoreMerger(num,score):
	return (num[0]+score[0],num[1]+score[1])
	
scores = wilmaAndFredScores.combineByKey(createScoreCombiner,scoreCombiner,scoreMerger)

print(scores.collect())

def averagingFunction(PersonScores):
	name = PersonScores[0]
	score = PersonScores[1][1]
	num = PersonScores[1][0]
	avg = score/num
	return (name,avg)
	
print(scores.values().collect())
#collectAsMap :this method should only be used if the resulting data is expected to be small, as all the data is loaded into the driver memory
#using collectAsMap throw out  takes exactly 1 argument (2 given)
#averageScores = scores.collectAsMap(averagingFunction)

averageScores = scores.map(averagingFunction)


print(averageScores.collect())
