import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from utilities.AvgCount import AvgCount

conf = SparkConf().setAppName("AvgPrice").setMaster("local[3]")
context = SparkContext(conf = conf)

lines = context.textFile("data/RealEstate.csv")
lines = lines.filter(lambda line: "Bedrooms" not in line)

prices = lines.map(lambda line: (line.split(",")[3], AvgCount(1, float(line.split(",")[2]))))
priceTotal = prices.reduceByKey(lambda x, y: AvgCount(x.count + y.count, x.total + y.total))

priceAvg = priceTotal.mapValues(lambda avgCount: avgCount.total / avgCount.count)
print("\nhousePriceAvg: ")
for bedroom, avg in priceAvg.collect():
    print("{} : {}".format(bedroom, avg))