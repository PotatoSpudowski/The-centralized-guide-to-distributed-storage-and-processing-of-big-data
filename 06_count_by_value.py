from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("collectHarry").setMaster("local[*]")
context = SparkContext(conf = conf)

f = open('data/harry.txt')
words = f.read().split()

wordRDD = context.parallelize(words)
print("Count: ",wordRDD.count())
wordCountByValue = wordRDD.countByValue()
print("Count by value: ")
for word, count in wordCountByValue.items():
    print(word, ": ", count)