from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

people = []
people.append({'name':'Amit', 'age':45,'gender':'M'}) 
people.append({'name':'Ganga', 'age':43,'gender':'F'}) 
people.append({'name':'John', 'age':28,'gender':'M'}) 
people.append({'name':'Lolita', 'age':33,'gender':'F'}) 
people.append({'name':'Dont Know', 'age':18,'gender':'T'})

print(people)

peopleRdd=sc.parallelize(people)
#Dictionary use following to get values
print(people[1]['age'])


resultRDD = peopleRdd.aggregate((0,0), lambda x,y: (x[0] + y['age'],x[1] + 1), lambda x,y: (x[0] + y[0], x[1] + y[1]))

print(resultRDD)