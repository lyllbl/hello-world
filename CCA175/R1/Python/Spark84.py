from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

spark.sql("select * from sparkdb.product where productcode is null").show()
spark.sql("select * from sparkdb.product where name like 'Pen%' order by price desc").show()
spark.sql("select * from sparkdb.product where name like 'Pen%' order by price desc,quantity").show()
spark.sql("select * from sparkdb.product where name like 'Pen%' order by price desc limit 2").show()