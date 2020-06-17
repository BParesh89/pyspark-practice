from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName('abc').getOrCreate()
sc = spark.sparkContext

# read files
orderItem = sc.textFile("/home/vagrant/Downloads/data-master/retail_db/order_items")

orderItemsMap = orderItem.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

# calculate revenue per orderId and create map with tab delimited data
revenuePerOrderId = orderItemsMap.reduceByKey(lambda a, b: round(a + b, 2)).\
    map(lambda r: str(r[0]) + "\t" + str(r[1]))

# save as text file
revenuePerOrderId.saveAsTextFile("/home/vagrant/Downloads/rddDemo/revenue_per_order_id")

# save as compressed file
# revenuePerOrderId.saveAsTextFile("/home/vagrant/Downloads/rddDemo/revenue_per_order_id_compressed",
#                                 compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")

# save as json
revenuePerOrderId.toDF().save("/home/vagrant/Downloads/rddDemo/revenue_per_order_id_json", "json")
