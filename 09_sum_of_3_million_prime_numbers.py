from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("prime").setMaster("local[*]")
context = SparkContext(conf = conf)
context.setLogLevel("ERROR")

first_mil = context.textFile("data/primes1.txt")
second_mil = context.textFile("data/primes2.txt")
third_mil = context.textFile("data/primes3.txt")

aggsRDD = first_mil.union(second_mil)
aggsRDD = aggsRDD.union(third_mil)

numbers = aggsRDD.flatMap(lambda line: line.split(" "))

validNumbers = numbers.filter(lambda num: num)
foatNumbers = validNumbers.map(lambda num: float(num))

sumNumber = foatNumbers.reduce(lambda x, y: x + y)

print("Sum of first 3 million prime numbers")
print(sumNumber)