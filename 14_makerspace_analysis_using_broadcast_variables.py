import sys
import time
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from utilities.Utils import Utils

def loadPostCodeMap():
    lines = open("data/uk-postcode.csv", "r").read().split("\n")
    splitsForLines = [Utils.COMMA_DELIMITER.split(line) for line in lines if line != ""]
    return {splits[0]: splits[7] for splits in splitsForLines}

def getPostPrefix(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    postcode = splits[4]
    return None if not postcode else postcode.split(" ")[0]

start_time = time.time()
conf = SparkConf().setAppName('UkMakerSpaces').setMaster("local[*]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
postCodeMap = sc.broadcast(loadPostCodeMap())
makerSpaceRdd = sc.textFile("data/uk-makerspaces-identifiable-data.csv")

regions = makerSpaceRdd \
    .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[0] != "Timestamp") \
    .filter(lambda line: getPostPrefix(line) is not None) \
    .map(lambda line: postCodeMap.value[getPostPrefix(line)] \
    if getPostPrefix(line) in postCodeMap.value else "Unknow")

for region, count in regions.countByValue().items():
    print(region, str(count))

print("\nExecution time")
print("--- %s seconds ---" % (time.time() - start_time))