from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName('abc').getOrCreate()
sc = spark.sparkContext

# read files
orderItem = sc.textFile("/home/vagrant/Downloads/data-master/retail_db/order_items")
orders = sc.textFile("/home/vagrant/Downloads/data-master/retail_db/orders")

# # countByKey used for getting count of orders statuswise
# orderCountByStatus = orders.map(lambda o: (o.split(",")[3], 1)).countByKey()
# print(orderCountByStatus.items())
#
# # groupByKey get sum of orders grouped by orderid
# orderItemsGrouped = orderItem.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4]))).groupByKey()
# print(orderItemsGrouped.map(lambda oig: (oig[0], round(sum(oig[1]), 2))).take(10))

#########################################
# NOTE - Always avoid use of groupByKey #
#########################################

# reduceByKey to get sum of orders grouped by order id
# it needs a reduce function to perform as transformation
orderItemsMap = orderItem.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
print(orderItemsMap.reduceByKey(lambda a, b: round(a + b, 2)).take(10))

# aggregateByKey requires 3 main inputs:
    # zeroValue: Initial value (mostly Zero (0)) which will not affect the aggregate values to be collected.
                # For example, 0 would be initial value to perform sum or count or to perform operation on String
                # then the initial value will be empty string.
    # Seq function: This function accepts two parameters. The second parameter is merged into the first parameter.
                         # This function combines/merges values within a single partition.
    # Combiner function: This function also accepts two parameters. Here parameters are merged into one across
                            # RDD partitions. Used for merging/combining output of seq function of multiple partitions

# # https://dbmstutorials.com/spark/spark-aggregate-by-key.html
# seqOp = eqOp = (lambda x, y: (x[0] + y, x[1] + 1))  # sequence function to be run on each RDD partition
# combOp = (lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1]))  # combiner functiont to merge output of sequence function processed on each RDD partition
# print(sc.parallelize(range(1, 32233)).repartition(2).aggregate((0, 0), seqOp, combOp))
#
# # find total revenue and count for each order id
# revenuePerOrder = orderItemsMap.repartition(2).aggregateByKey((0, 0), seqOp, combOp)
# print(revenuePerOrder.take(5))
#
# # sortByKey -- print results by sorted Key ascending order id
# print(revenuePerOrder.sortByKey(False).take(5))


