from pyspark.sql import SparkSession


# create spark session
spark = SparkSession.builder.master("local[*]").appName('hive').getOrCreate()

# load order file in spark dataframe with schema
orders = spark.read.format('csv').\
    schema('order_id int, order_date string, order_customer_id int, order_status string').\
    load("file:/home/vagrant/Downloads/data-master/retail_db/orders") # use file:<full path> practice if we dont use
                                                                    # can create problem while running via spark submit

# load order_items file in dataframe
orderItems = spark.read.format('csv').\
    schema('order_item_id int, order_item_order_id int, order_item_product_id int, order_item_quantity int, '
           'order_item_subtotal float, order_item_product_price float').\
    load("file:/home/vagrant/Downloads/data-master/retail_db/order_items")

# use of filter or where - filter and where works same

closedorders = orders.where(orders.order_status == 'CLOSED')
filterorders = orders.filter(orders.order_status == 'CLOSED')
closedorders.show()
filterorders.show()

# print count of common records using dataframe join
print(orders.join(orderItems, orders.order_id == orderItems.order_item_order_id).count())

# aggregation - group by and taking sum based on order_item_subtotal
from pyspark.sql.functions import *
orders.where('order_status in ("COMPLETED", "CLOSED")').\
      join(orderItems, orders.order_id == orderItems.order_item_order_id).\
      groupBy('order_date', 'order_item_product_id').\
      agg(round(sum('order_item_subtotal'), 2).alias('revenue')).show(truncate=False)

# order by or sort

orders.sort('order_date').show()
orders.orderBy(orders.order_date.desc()).show()

# sort first column in descending and second in ascending
orders.sort(['order_date', 'order_status'], ascending=[0, 1]).show()