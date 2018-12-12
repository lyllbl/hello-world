from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

EmployeeNameRDD = sc.textFile("hdfs://master:9000/sparkData/input/EmployeeName.csv")

EmployeeNameFirstColumnRDD = EmployeeNameRDD.map(lambda line:line.split(',')).map(lambda line:(line[1],line[0]))

EmployeeNameFirstColumnSortRDD = EmployeeNameFirstColumnRDD.sortByKey()

resultRDD = EmployeeNameFirstColumnSortRDD.map(lambda (x,y):(y,x))

print(resultRDD.take(5))

resultRDD.repartition(1).saveAsTextFile("hdfs://master:9000/sparkData/output/35/RDD")
