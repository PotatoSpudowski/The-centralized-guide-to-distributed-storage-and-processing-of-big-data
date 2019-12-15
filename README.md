# hitchhikers-guide-to-distributed-storage-and-processing-of-big-data

![pyspark](https://github.com/PotatoSpudowski/hitchhikers-guide-to-distributed-storage-and-processing-of-big-data/blob/master/Images/spark.png)

This is a repository containing my code samples that helped me understand the concepts of distributed storage and processing of Big data using Apache spark and Python.

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