from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName('abc').getOrCreate()
sc = spark.sparkContext

# read files
orders = sc.textFile("/home/vagrant/Downloads/data-master/retail_db/orders")
customer = sc.textFile("/home/vagrant/Downloads/data-master/retail_db/customers")
uniqueCust = orders.map(lambda rec: rec.split(",")[2]).distinct().collect()

# filter customers who have not ordered
nonOrderingCust = customer.filter(lambda rec: rec.split(",")[0] not in uniqueCust)\
    .map(lambda rec: ((rec.split(",")[2], rec.split(",")[1]), str(rec.split(",")[2]) + "," + str(rec.split(",")[1])))\
    .sortByKey()\
    .map(lambda rec: rec[1])

nonOrderingCust.saveAsTextFile(path="hdfs://localhost:9000/usr/solutions/solution_2/inactivecustomer")

# print("\n")
# for data in customer.take(3):
#     print(data)

# BoltonMary
# EllisonAlbert
# GreenCarolyn
# GreeneMary
# HarrellMary
