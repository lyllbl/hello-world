from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

orders = spark.read.jdbc("jdbc:mysql://localhost:3306/retail_db",table = 'orders',properties = {'user':'retail_dba','password':'cloudera'})
print(orders.show())
orderitems = spark.read.jdbc("jdbc:mysql://localhost:3306/retail_db",table = 'order_items',properties = {'user':'retail_dba','password':'cloudera'})
print(orderitems.show())
join = orders.join(orderitems,orders.order_id == orderitems.order_item_order_id,'inner')
print(join.show())

join.registerTempTable("join")

output = spark.sql("select order_date,order_customer_id,sum(order_item_subtotal) as revenue from join group by order_date,order_customer_id")
print(output.show())

sum = join.select("order_customer_id","order_item_subtotal").groupBy("order_customer_id").sum("order_item_subtotal")
print(sum.show())
output = sum.groupBy("order_customer_id").max("sum(order_item_subtotal)")
print(output.show())