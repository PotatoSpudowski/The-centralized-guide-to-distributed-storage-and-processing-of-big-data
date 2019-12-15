import random 
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("reduce").setMaster("local[*]")
context = SparkContext(conf = conf)

nums = [random.randrange(1, 101, 1) for _ in range(10000)]

numsRDD = context.parallelize(nums)

product = numsRDD.reduce(lambda x, y: x * y)
print(product)