from pyspark.sql import SparkSession, SQLContext

# create spark session
spark = SparkSession.builder.appName('abc').getOrCreate()
sc = spark.sparkContext

# read textfile
print(sc.textFile("/home/vagrant/Downloads/data-master/retail_db/products/part-00000").first())
print("\n")

# read json file using read.load -- need to pass format like json, parquet etc

# create SQLcontext object and pass sparkContext to it
sqc = SQLContext(sc)
print(sqc.read.load("/home/vagrant/Downloads/data-master/retail_db_json/order_items", "json").first())
print("\n")

# read json file using read.json
print(sqc.read.json("/home/vagrant/Downloads/data-master/retail_db_json/order_items").first())

