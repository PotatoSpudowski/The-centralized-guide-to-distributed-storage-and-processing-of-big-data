from pyspark import SparkContext, SparkConf

def isNotHeader(line):
    return not(line.startswith("host") and "bytes" in line)

conf = SparkConf().setAppName("NasaLogs").setMaster("local[*]")

contxt = SparkContext(conf = conf)

log1 = contxt.textFile("data/nasa_19950701.tsv")
log2 = contxt.textFile("data/nasa_19950801.tsv")

aggLogs = log1.union(log2)
aggLogs = aggLogs.filter(isNotHeader)

logSample = aggLogs.sample(withReplacement = True, fraction = 0.2)

logSample.saveAsTextFile("Output/sample_logs_nasa.csv")