from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")
orderitems = spark.read.jdbc("jdbc:mysql://localhost:3306/retail_db",table = 'order_items',properties = {'user':'retail_dba','password':'cloudera'})
print(orderitems.show())

#using groupBy() to sum, if there nothint to group
r1 = orderitems.groupBy().sum("order_item_subtotal")
print(r1.show())
r2 = orderitems.groupBy().max("order_item_subtotal")
print(r2.show())
r3 = orderitems.groupBy().min("order_item_subtotal")
print(r3.show())
r4 = orderitems.groupBy().avg("order_item_subtotal")
print(r4.show())