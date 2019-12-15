from pyspark.sql import SparkSession

session = SparkSession.builder.appName("soAnalysis").getOrCreate()

dataFrameReader = session.read

responses = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("data/2016-stack-overflow-survey-responses.csv")

print("=== Print out schema ===")
responses.printSchema()

responses1 = responses.select(
        "country",
        "occupation",
        "age_midpoint",
        "salary_midpoint")
responses1.show()

responses1.filter(responses1["country"] == "Canada").show()

groupedData = responses1.groupBy("occupation")
groupedData.count().show()

responses1.filter(responses1["age_midpoint"] < 20).show()

responses1.orderBy(responses1["salary_midpoint"], ascending = False).show()

groupByCountry = responses1.groupBy("country")
groupByCountry.avg("salary_midpoint").show()

responses2 = responses.withColumn("salary_midpoint_bucket", ((responses["salary_midpoint"]/20000).cast("integer")*20000))
responses2.printSchema()
responses2.select("salary_midpoint", "salary_midpoint_bucket").show()

responses2 \
        .groupBy("salary_midpoint_bucket") \
        .count() \
        .orderBy("salary_midpoint_bucket") \
        .show()

session.stop()
