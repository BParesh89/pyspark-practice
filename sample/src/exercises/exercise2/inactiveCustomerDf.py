from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# create spark session
spark = SparkSession.builder.master("local[*]").appName('inactiveCustomer').getOrCreate()

orderschema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True)])

orderDf = spark.read.load("/home/vagrant/Downloads/data-master/retail_db/orders", format='csv',
                          sep=',', schema=orderschema)

customerschema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_fname", StringType(), True),
    StructField("customer_lname", StringType(), True)])

customerDf = spark.read.load("/home/vagrant/Downloads/data-master/retail_db/customers", format='csv',
                          sep=',', schema=customerschema)

distinctCustOrder = orderDf.select("order_customer_id").distinct().withColumnRenamed("order_customer_id","customer_id")

customerDf.join(distinctCustOrder, on="customer_id", how="leftanti")\
    .select("customer_lname", "customer_fname")\
    .sort("customer_lname", "customer_fname").show()


