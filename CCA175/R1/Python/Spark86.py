from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

spark.sql("select max(quantity) as max_quantity,min(quantity) as min_quantity, avg(quantity) as avg_quantity,stddev_pop(quantity) as stddev, std(quantity) as std,sum(quantity) as sum_quantity from sparkdb.product").show()

spark.sql("select productcode,max(price) as max_price,min(price) as min_price from sparkdb.product group by productcode").show()

spark.sql("select productcode,max(quantity) as max_quantity,min(quantity) as min_quantity, round(avg(quantity),2) as avg_quantity, round(std(quantity),2) as std,sum(quantity) as sum_quantity from sparkdb.product group by productcode").show()

spark.sql("select productcode,avg(price) as avg_price from sparkdb.product group by productcode having count(1) >2").show()


#rollup to across all the products
spark.sql("select nvl(productcode,'All') as code,max(quantity) as max_quantity,min(quantity) as min_quantity,avg(quantity) as avg_quantity, sum(quantity) as sum_quantity from sparkdb.product group by productcode with rollup").show()