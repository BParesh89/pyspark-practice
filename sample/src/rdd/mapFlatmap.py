from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName('abc').getOrCreate()
sc = spark.sparkContext

# read file
orders = sc.textFile("/home/vagrant/Downloads/data-master/retail_db/orders")
# get status of first order
print(orders.map(lambda o: o.split(",")[3]).first())
print("\n")

# lines list
linesList = ["how are you", "I am fine", " I hope you are fine too"]
lines = sc.parallelize(linesList)
# split each line into words
words = lines.flatMap(lambda w: w.split(" "))
# append 1 to each word and create tuple
wordTuple = words.map(lambda word: (word, 1))

# get wordcount for each word
wordCount = wordTuple.reduceByKey(lambda a, b: a + b)
# print(wordCount.take(5))

# convert to spark DataFrame
print(spark.createDataFrame(wordCount).show(10))

