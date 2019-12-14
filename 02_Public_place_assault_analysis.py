import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from utilities.Utils import Utils


conf = SparkConf().setAppName("assaults").setMaster("local[4]")
sc = SparkContext(conf = conf)

assualts = sc.textFile("data/analysis-public-place-assaults-sexual-assaults-and-robberies-2015-csv.txt")

assualts_minor_area = assualts \
    .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[9]=="Minor urban area")

assualts_minor_area.saveAsTextFile("Output/assualts_minor_area.txt")