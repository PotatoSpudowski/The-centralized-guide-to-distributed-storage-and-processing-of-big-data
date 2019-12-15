import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from utilities.Utils import Utils

def filterResponses(response):
    processedBytes.add(len(response.encode('utf-8')))
    cells = Utils.COMMA_DELIMITER.split(response)
    tot.add(1)
    if not cells[14]:
        missing.add(1)
    return cells[2] == "Argentina"

conf = SparkConf().setAppName('SO').setMaster("local[*]")
context = SparkContext(conf = conf)

tot = context.accumulator(0)
missing = context.accumulator(0)
processedBytes = context.accumulator(0)

dataRDD = context.textFile("data/2016-stack-overflow-survey-responses.csv")

responsesFromArgentina = dataRDD.filter(filterResponses)


print("Count of responses from Argentina: ", str(responsesFromArgentina.count()))
print("Total count of responses: ", str(tot.value))
print("Total no of missing salary responses: ", str(missing.value))
print("Total no of Bytes processed: ", str(processedBytes.value))