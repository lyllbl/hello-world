from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

spark.sql("select productid as id,productcode as code,name as Description, price as UnitPrice from sparkdb.product ").show()
#could not use operate || to concatnate
spark.sql("select concat(productcode,'-',name) as ProductDescription from sparkdb.product ").show()
spark.sql("select distinct price from sparkdb.product ").show()
print("Distinct column")
spark.sql("select distinct price,name from sparkdb.product ").show()
print("Distinct funcation")
spark.sql("select distinct(price,name) as dis from sparkdb.product ").show()
spark.sql("select price from sparkdb.product order by productcode,productid").show()
spark.sql("select count(1) as countnum from sparkdb.product ").show()
spark.sql("select productcode,count(1) as countnum from sparkdb.product group by productcode").show()