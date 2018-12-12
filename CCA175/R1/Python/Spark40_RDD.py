from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")




def IfReturnZero(x):
	if len(str(x)) == 0: 
		return '0'
	else:
		return x




spark15file1RDD = sc.textFile("hdfs://master:9000/sparkData/input/spark15file1.txt") 

spark15file1SplitRDD = spark15file1RDD.map(lambda line:line.split(','))


#https://www.tutorialspoint.com/How-to-create-a-lambda-inside-a-Python-loop weibo
#http://www.u.arizona.edu/~erdmann/mse350/topics/list_comprehensions.html#lambda List Comprehensions
#https://docs.python.org/3/tutorial/datastructures.html#list-comprehensions 5.1.3. List Comprehensions

#https://docs.python.org/3/reference/expressions.html#grammar-token-conditional-expression 6.12 Conditional expressions (if in lambda is ternary operator)
#https://gerardnico.com/lang/python/grammar/lambda#if 7.2 -if


#https://pythonconquerstheuniverse.wordpress.com/2011/08/29/lambda_tutorial/ lambda confusing

spark15file1ForeachRDD = spark15file1SplitRDD.map(lambda fields:[IfReturnZero(i) for i in fields])

print(spark15file1ForeachRDD.collect())

spark15file1ForeachRDD.saveAsTextFile("hdfs://master:9000/sparkData/output/40/RDD")

#spark15file1ForeachRDD = spark15file1SplitRDD.foreach(lambda word :IfReturnZero(word))
#print(spark15file1RDD.collect())
#print(spark15file1RDD.foreach(lambda word: print(word)))