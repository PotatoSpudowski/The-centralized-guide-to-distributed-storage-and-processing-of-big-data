from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("AverageHousePrice").setMaster("local[*]")
context = SparkContext(conf = conf)

lines = context.textFile("data/RealEstate.csv")
lines = lines.filter(lambda line: "Bedrooms" not in line)

prices = lines.map(lambda line: (line.split(",")[3], float(line.split(",")[2])))

createCombiner = lambda x: (x, 1)
mergeValue = lambda value, x: (value[0] + x, value[1] + 1)
mergeCombiners = lambda valueA, valueB: (valueA[0] + valueB[0], valueA[1] + valueB[1])

priceTotal = prices.combineByKey(
    createCombiner,
    mergeValue,
    mergeCombiners )

averageByKey = priceTotal.mapValues(lambda key: key[0]/key[1])

sortedAverage =  averageByKey.sortBy(lambda avg: avg[1], ascending=False)

for rooms, avgprice in sortedAverage.collect():
    print(rooms, avgprice)