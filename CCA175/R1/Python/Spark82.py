from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

product = spark.sql("select name , quantity from sparkdb.product where quantity <= 2000")
print(product.show())
spark.sql("select name , price from sparkdb.product where productCode = 'PEN'").show()
spark.sql("select name , price from sparkdb.product where upper(name) like 'PENCIL%'").show()
spark.sql("select name , price from sparkdb.product where upper(name) like 'P%'").show()