# hitchhikers-guide-to-distributed-storage-and-processing-of-big-data

## Introduction
![pyspark](https://github.com/PotatoSpudowski/hitchhikers-guide-to-distributed-storage-and-processing-of-big-data/blob/master/Images/spark.png)

This is a repository containing my code samples that helped me understand the concepts of distributed storage and processing of Big data using Apache spark and Python.

![elephant](https://github.com/PotatoSpudowski/hitchhikers-guide-to-distributed-storage-and-processing-of-big-data/blob/master/Images/hadoop_spark_logos.png)

Let us first address the elephant in the room(Pun intended)

#### What is Big Data?
Big Data refers to the data which is gigantic in size. This data is generated from various different sources, comes up in different formats and at a very high velocity. Big Data is generally in the order of petabytes.

One of the biggest challenges with respect to Big Data is analyzing the data. There are multiple solutions available to do this. The most popular one is Apache Hadoop.

#### Hadoop
Apache Hadoop is an open-source framework written in Java that allows us to store and process Big Data in a distributed environment, across various clusters of computers using simple programming constructs. To do this, Hadoop uses an algorithm called MapReduce, which divides the task into small parts and assigns them to a set of computers. Hadoop also has its own file system, Hadoop Distributed File System (HDFS), which is based on Google File System (GFS).

#### Spark
Apache Spark is an open-source distributed cluster-computing framework. Spark is a data processing engine developed to provide faster and easy-to-use analytics than Hadoop MapReduce. Before Apache Software Foundation took possession of Spark, it was under the control of University of California, Berkeley’s AMP Lab.

#### How Spark Is Better than Hadoop?
* In-memory Processing: In-memory processing is faster when compared to Hadoop, as there is no time spent in moving data/processes in and out of the disk. Spark is 100 times faster than MapReduce as everything is done here in memory.

* Stream Processing: Apache Spark supports stream processing, which involves continuous input and output of data. Stream processing is also called real-time processing.

* Less Latency: Apache Spark is relatively faster than Hadoop, since it caches most of the input data in memory by the Resilient Distributed Dataset (RDD). RDD manages distributed processing of data and the transformation of that data. This is where Spark does most of the operations such as transformation and managing the data. Each dataset in an RDD is partitioned into logical portions, which can then be computed on different nodes of a cluster.

* Lazy Evaluation: Apache Spark starts evaluating only when it is absolutely needed. This plays an important role in contributing to its speed.

* Less Lines of Code: Although Spark is written in Scala and Java, the implementation is in Scala, so the number of lines are relatively lesser in Spark when compared to Hadoop.

#### Spark Architecture
![SparkArchitecture](https://github.com/PotatoSpudowski/hitchhikers-guide-to-distributed-storage-and-processing-of-big-data/blob/master/Images/spark_architecture.png)

Spark Architecture includes following three main components:

* Data Storage
* API
* Management Framework

Data Storage:

Spark uses HDFS file system for data storage purposes. It works with any Hadoop compatible data source including HDFS, HBase, Cassandra, etc.

API:

The API provides the application developers to create Spark based applications using a standard API interface. Spark provides API for Scala, Java, and Python programming languages.

Resource Management:

Spark can be deployed as a Stand-alone server or it can be on a distributed computing framework like Mesos or YARN.

#### What are Resilient Distributed Datasets?

Resilient Distributed Dataset (based on Matei’s research paper) or RDD is the core concept in Spark framework. Think about RDD as a table in a database. It can hold any type of data. Spark stores data in RDD on different partitions.

They help with rearranging the computations and optimizing the data processing.

They are also fault tolerance because an RDD know how to recreate and recompute the datasets.

RDDs are immutable. You can modify an RDD with a transformation but the transformation returns you a new RDD whereas the original RDD remains the same.

RDD supports two types of operations:

* Transformation
* Action

Transformation: 

Transformations don't return a single value, they return a new RDD. Nothing gets evaluated when you call a Transformation function, it just takes an RDD and return a new RDD.

Some of the Transformation functions are 
* map 
* filter 
* flatMap 
* groupByKey
* reduceByKey
* aggregateByKey
* pipe
* coalesce.

Action: 

Action operation evaluates and returns a new value. When an Action function is called on a RDD object, all the data processing queries are computed at that time and the result value is returned.

Some of the Action operations are 
* reduce 
* collect
* count
* first
* take
* countByKey 
* foreach.

## Installing Spark

Prerequisites: Java and Git

Go to this link and download Spark:

https://spark.apache.org/downloads.html

Then open terminal and extract the files:

```
$ cd Downloads
$ tar -xvf spark-2.3.3-bin-hadoop2.7.tgz
```

Now the Spark directory will be extracted.
Next, go to the bin directory in the spark directory:
```
$ cd spark-2.3.3-bin-hadoop2.7/bin/
```
And to start spark shell, run the following command:
```
$ ./spark-shell
```
## Code samples

### 01_counter.py
This script loads the "Harry Potter and the Prisoner of Azkaban" book using pyspark modules 
and finds the count of each word in the book by loading the text file as a RDD(Resilient distributed dataset) and distributing the processing among 3 local clusters.

### 02_Public_place_assault_analysis.py
This script loads the "analysis-public-place-assaults-sexual-assaults-and-robberies-2015-csv.txt" data from the data folder as a RDD and performs filtering of the crime instances limited to only "Minor urban area" using 4 local clusters for processing. The output RDD is saved in the Output folder.

### 03_NASA_logs_sampling.py
This script loads the log files colleted from the NASA's Apache web server and aggregates the logs. Finds the sample of the aggregrated log file and saves it as a RDD in the Output folder.

### 04_NASA_logs_intersection.py
This script loads the log files and finds the intersection based on host name. It finally saves the RDD in the Output folder containg the list of host names that appear in both the log files.

### 05_collect_harry.py
This script loads the "Harry Potter and the Prisoner of Azkaban" book and converst it into a list of words. Using the "Parallelize" function it creates RDDs by utilizing all the available local clusters. Then using the "Collect" functions it collects and combines all the RDDs into a single object.

### 06_count_by_value.py
This script loads the "Harry Potter and the Prisoner of Azkaban" book and converts it to a list of words. Using the "Parallelize" function it creates RDDs by utilizing all the available local clusters. Then using the "Count" function finds the total no of words in the RDD and using the "countByValue" function creates a map of words and it's count.

### 07_take_harry.py
This script loads the "Harry Potter and the Prisoner of Azkaban" book and converts it to a list of words. Using the "Parallelize" function it creates RDDs by utilizing all the available local clusters. Then using the "take" function we can peak into the RDD and get 5 words. Note the order of the words returned from the RDD are in the same order as the input file.

### 08_reduce.py
This script creates a list of random integers, converts them into RDD and uses the "Reduce" function to get the product of the integers in the RDD.

## Resources
https://intellipaat.com/blog/what-is-apache-spark/

https://www.infoq.com/articles/apache-spark-introduction/

https://people.eecs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf

https://data-flair.training/blogs/install-spark-ubuntu/

