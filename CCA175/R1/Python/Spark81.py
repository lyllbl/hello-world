from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

productDF = spark.read.csv("hdfs://master:9000/sparkData/input/product.csv", sep = ',' , header = True , inferSchema = True)
print(productDF.show(20))
print(productDF.printSchema())
#spark.sql don't add ; in the sentence
spark.sql("drop table if exists sparkdb.productOrc")
spark.sql("create table sparkdb.productOrc(productID integer,productCode string, name string, quantity integer, price double)STORED as ORC")


#source file header must be lowercase ,otherwise that will be null value. parquet type don't care about it.
productDF.write.saveAsTable("sparkdb.productOrc", format = 'ORC' , mode = 'overwrite')

spark.sql("drop table if exists sparkdb.productOrc_ext")
spark.sql("create external table sparkdb.productOrc_ext(productID integer,productCode string, name string, quantity integer, price double)STORED as ORC location '/user/hive/warehouse/external/productOrc_ext'")
#for external table must add path option. LOCATION is mandatory for EXTERNAL tables
productDF.write.mode("overwrite").format("orc").option('path','/user/hive/warehouse/external/productOrc_ext').saveAsTable("sparkdb.productOrc_ext")
#productDF.write.saveAsTable("sparkdb.productOrc" , mode = 'overwrite')

#saveAsTable could create managed table in hive
spark.sql("drop table if exists sparkdb.productParquet")
productDF.write.saveAsTable("sparkdb.productParquet" , mode = 'overwrite')