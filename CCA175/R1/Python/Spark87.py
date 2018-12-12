from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

product = spark.read.csv("hdfs://master:9000/sparkData/input/product87.csv", sep = ',' , header = True, inferSchema = True)
supplier = spark.read.csv("hdfs://master:9000/sparkData/input/supplier.csv", sep = ',' , header = True, inferSchema = True)
mapping = spark.read.csv("hdfs://master:9000/sparkData/input/products_suppliers.csv", sep = ',' , header = True, inferSchema = True)


#registerTempTable is new in spark 1.3; createTempView or createOrReplaceTempView is new in spark 2.0
product.registerTempTable("product")
supplier.registerTempTable("supplier")
mapping.registerTempTable("mapping")

spark.sql("select product.name as pro_product,product.price as pro_price, supplier.name as sup_name from product inner join supplier on product.supplierid = supplier.supplierid where product.price < 0.6").show()