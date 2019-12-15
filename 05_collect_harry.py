from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("collectHarry").setMaster("local[*]")
context = SparkContext(conf = conf)

f = open('data/harry.txt')
words = f.read().split()

wordRDD = context.parallelize(words)

words2 = wordRDD.collect()

for word in words2:
    print(word)