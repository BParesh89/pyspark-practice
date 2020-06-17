from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName('abc').getOrCreate()

# create dataFrame
df = spark.createDataFrame([("2016-03-11 09:00:07", 1)]).toDF("date", "val")
df.show()
df.printSchema()
