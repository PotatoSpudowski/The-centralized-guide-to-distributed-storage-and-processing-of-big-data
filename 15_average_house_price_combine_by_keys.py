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
for rooms, avgprice in averageByKey.collect():
    print(rooms, avgprice)
