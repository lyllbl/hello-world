from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

#properties : a dictionary of JDBC database connection arguments. Normally at least properties user and password with their corresponding values. For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }
orders =  spark.read.jdbc("jdbc:mysql://localhost:3306/retail_db",table = 'orders',properties = {'user':'retail_dba','password':'cloudera'})
print(orders.show())
orderitems = spark.read.jdbc("jdbc:mysql://localhost:3306/retail_db",table = 'order_items',properties = {'user':'retail_dba','password':'cloudera'})
print(orderitems.show())
joinDF = orders.join(orderitems,orders.order_id == orderitems.order_item_order_id,'inner')
print(joinDF.show())


#why amount collected on this order is sum("order_item_subtotal")
r1 = joinDF.groupBy("order_id","order_date").count()
print(r1.show())

r2 = joinDF.groupBy("order_date").count().sort("order_date")
print(r2.show())
