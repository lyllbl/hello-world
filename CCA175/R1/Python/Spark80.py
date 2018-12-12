from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

products = spark.read.jdbc("jdbc:mysql://localhost:3306/retail_db",table = 'products', properties = {'user':'retail_dba','password':'cloudera'})
print(products.show())

products.registerTempTable("products")
output = spark.sql("select product_category_id,product_price from products  order by product_category_id,product_price")
print(output.show())