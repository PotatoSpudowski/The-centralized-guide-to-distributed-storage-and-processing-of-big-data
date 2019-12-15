from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("JoinOperations").setMaster("local[1]")
context = SparkContext(conf = conf)

os = context.parallelize([("linux", "10"), ("windows", "0")])
companies = context.parallelize([("linux", "NONE"), ("windows", "Microsoft")])

join =  os.join(companies)
join.saveAsTextFile("Output/os_companies_join.text")

leftOuterJoin = os.leftOuterJoin(companies)
leftOuterJoin.saveAsTextFile("Output/os_companies_leftOuterJoin.text")

rightOuterJoin = os.rightOuterJoin(companies)
rightOuterJoin.saveAsTextFile("Output/os_companies_rightOuterJoin.text")

fullOuterJoin = os.fullOuterJoin(companies)
fullOuterJoin.saveAsTextFile("Output/os_companies_fullOuterJoin.text")