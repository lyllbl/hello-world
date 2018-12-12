from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

products = sc.textFile("hdfs://master:9000/user/root/p93_products")

r1 = products.map(lambda record:record.split(",")).map(lambda columns:(columns[1],0.0 if columns[4] == '' else float(columns[4])))
print(r1.take(5))

r2 =  r1.groupByKey()
print(r2.mapValues(list).take(5))
#sorted is python original funcation and first argument should be Iterable object
print("sorted")
r3 = r2.map(lambda keyValues:(keyValues[0],sorted(keyValues[1],reverse = True)))
print(r3.mapValues(list).take(5))
