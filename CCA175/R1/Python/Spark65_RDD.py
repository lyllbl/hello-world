from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize(["dog", "cat", "owl", "gnu", "ant"], 2)
b = sc.parallelize(range(1,6),2)
c = a.zip(b)
print(a.collect())
print(b.collect())
print(c.collect())

#['dog', 'cat', 'owl', 'gnu', 'ant']
#[1, 2, 3, 4, 5]
#[('dog', 1), ('cat', 2), ('owl', 3), ('gnu', 4), ('ant', 5)]


output = c.sortByKey(False)
print(output.collect())