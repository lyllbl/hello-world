from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

course = spark.read.csv("hdfs://master:9000/sparkData/input/course.txt", sep = ',' , header = True , inferSchema = True)
fee = spark.read.csv("hdfs://master:9000/sparkData/input/fee.txt", sep = ',' , header = True , inferSchema = True)
course.createOrReplaceTempView("course")
fee.createOrReplaceTempView("fee")
spark.sql("select * from course left join fee on course.id = fee.id").show()
spark.sql("select * from course right join fee on course.id = fee.id").show()
spark.sql("select * from course left join fee on course.id = fee.id where fee.id is not null").show()