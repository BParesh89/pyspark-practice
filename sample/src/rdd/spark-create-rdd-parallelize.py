from pyspark.sql import SparkSession

l = range(1, 10000)

# create spark session
spark = SparkSession.builder.appName('abc').getOrCreate()
sc = spark.sparkContext

# use parallelize to convert to RDD
lRDD = sc.parallelize(l)

# read file and split line
productRaw = open("/home/vagrant/Downloads/data-master/retail_db/products/part-00000").read().splitlines()
print(type(productRaw))

# convert to RDD
productsRDD = sc.parallelize(productRaw)
print("\n")
print(type(productRaw))

print("\n")
print(productsRDD.first())

print(productsRDD.count())

# /usr/lib/jvm/java-8-openjdk-amd64/jre

