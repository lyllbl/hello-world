from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

spark.sql("select * from sparkdb.product where quantity >= 5000 and name like 'Pen%'").show()
spark.sql("select * from sparkdb.product where quantity >= 5000 and name like 'Pen%' and price < 1.24").show()
spark.sql("select * from sparkdb.product where quantity < 5000 and name not like 'Pen%'").show()
spark.sql("select * from sparkdb.product where name in ('Pen Red', 'Pen Black')").show()
spark.sql("select * from sparkdb.product where (price between 1.0 and 2.0) and (quantity between 1000 and 2000)").show()