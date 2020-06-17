from functools import reduce

from pyspark.sql import *

# open file & read
orderItemFile = open("/home/vagrant/Downloads/data-master/retail_db/order_items/part-00000").read()
# split file by each line
orderItem = orderItemFile.splitlines()  # return a list
# print(orderItem[0])
# get records where orderid is 68880
filteredOrderItem = filter(lambda x : int(x.split(",")[1]) == 68880, orderItem)  # returns a iterable filter object
# print(list(filteredOrderItem))
# convert revenue for each record in above step to float
orderItemMap = map(lambda y : float(y.split(",")[4]), filteredOrderItem)
# print(list(orderItemMap))
# sum all revenue
orderItemRevenue = reduce(lambda total, element: total + element, orderItemMap)
print(orderItemRevenue)