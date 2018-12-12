from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

patient = spark.read.csv("hdfs://master:9000/sparkData/input/patient.csv", sep = ',' , header = True, inferSchema = True)
patient.createOrReplaceTempView("patient")
spark.sql("select * from patient where lastVisitDate between '2012-09-15' and current_date()").show()
spark.sql("select * from patient where year(dateOfBirth) = 2011").show()
spark.sql("select *,datediff(now(),dateOfBirth)/365 as age from patient").show()
spark.sql("select * from patient where datediff(now(),lastVisitDate)>60").show()
spark.sql("select * from patient where datediff(now(),dateOfBirth)/365 <= 18").show()