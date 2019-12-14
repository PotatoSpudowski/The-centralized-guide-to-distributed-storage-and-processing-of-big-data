"""
This script loads the "Harry Potter and the Prisoner of Azkaban" book using pyspark modules 
and finds the count of each word in thSe book.
"""

from pyspark import SparkContext

context = SparkContext("local[3]", "App no 1") #local[3] means that the SparkContext context can utilize 3 local cores.

lines = context.textFile("data/harry.txt") #context.textFile() is used to load RDD(Resilient distributed dataset) from a text file.

words = lines.flatMap(lambda line: line.split(" ")) #flatMap is like map function but is used on the RDD.

wordCounts = words.countByValue()
maxWord = ""
maxcount = 0
for word, count in wordCounts.items():
    print(word, count)
    if count > maxcount:
        maxWord, maxcount = word, count

print("\n Most occuring word!\n")
print(maxWord, maxcount)