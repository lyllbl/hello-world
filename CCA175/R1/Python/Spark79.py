from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

products = spark.read.jdbc("jdbc:mysql://localhost:3306/retail_db",table = 'products',properties = {'user':'retail_dba','password':'cloudera'})
print(products.show())

r1 = products.filter("product_price <>0")
print(r1.show())
print("ascending")
sortAsc = r1.sort("product_price",ascending = True)
print(sortAsc.show())
print("descending")
sortDeasc = r1.sort(r1.product_price.desc())
print(sortDeasc.show())
print("price ascending,id descending")
sortID = r1.sort(["product_price","product_id"],ascending = [0,1])
print(sortID.show())