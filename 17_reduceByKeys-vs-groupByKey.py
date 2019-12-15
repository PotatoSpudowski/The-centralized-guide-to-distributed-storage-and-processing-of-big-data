from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("svg").setMaster("local[*]")
context = SparkContext(conf = conf)
context.setLogLevel("ERROR")

words = ["harry", "potter", "potter", "hermoine", "hermoine"]
wordPair = context.parallelize(words)

wordPair = wordPair.map(lambda word: (word, 1))

wordCount = wordPair \
    .reduceByKey(lambda x, y: x + y) \
    .collect()

print(wordCount)

wordCount = wordPair \
    .groupByKey() \
    .mapValues(len) \
    .collect()

print(wordCount)
