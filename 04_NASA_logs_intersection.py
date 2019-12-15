from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("intersetHosts").setMaster("local[1]")
context = SparkContext(conf = conf)

log1 = context.textFile("data/nasa_19950701.tsv")
log2 = context.textFile("data/nasa_19950801.tsv")

log1 = log1.map(lambda line: line.split("\t")[0])
log2 = log2.map(lambda line: line.split("\t")[0])

intersection_log = log1.intersection(log2)
cleaned_intersection_log = intersection_log.filter(lambda host: host != "host")
cleaned_intersection_log.saveAsTextFile("Output/nasa_logs_hosts.csv")