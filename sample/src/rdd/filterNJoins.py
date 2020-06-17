from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName('abc').getOrCreate()
sc = spark.sparkContext

# read files
orderItem = sc.textFile("/home/vagrant/Downloads/data-master/retail_db/order_items")
orders = sc.textFile("/home/vagrant/Downloads/data-master/retail_db/orders")

###################
###    filter   ###
###################

# filter records with order_id 2
filteredOrder = orderItem.filter(lambda o: int(o.split(",")[1]) == 2)
print("Records with order id 2\n")
print(filteredOrder.take(3))
print("\n")
print("Orders with minimum order item subtotal for given order id")
print(filteredOrder.reduce(lambda x, y: x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y))

###################
###    Joins    ###
###################

# inner join
orders = sc.textFile("/home/vagrant/Downloads/data-master/retail_db/orders")
orderMap = orders.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderItemMap = orderItem.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
print("inner join\n", orderMap.join(orderItemMap).count())

# left outer join
print("left outer join\n", orderMap.leftOuterJoin(orderItemMap).count())

# right outer Join
print("right outer join\n", orderMap.rightOuterJoin(orderItemMap).count())



