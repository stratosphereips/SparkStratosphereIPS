__author__ = 'username'

import sys
import os
from pyspark.context import SparkContext
import datetime

DATE_FORMAT = '%Y.%m.%d'
start = datetime.datetime.strptime('2017.03.01', DATE_FORMAT)
end = datetime.datetime.strptime('2017.03.31', DATE_FORMAT)
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]

filenames = []
hdfs_dir = 'hdfs:///user/ykasimov/CTU-Flows/'
for date in date_generated:
    str_date = date.strftime(DATE_FORMAT)
    file = hdfs_dir + str_date + '.binetflow'
    filenames.append(file)

sc = SparkContext()
myRdd = sc.textFile(','.join(filenames))

# ip_count = myRdd.map(lambda line: line.split(',')).filter(lambda line: len(line) > 7).filter(
#     lambda line: str(line[6]).startswith('147.32')).map(
#     lambda line: (tuple([line[3], line[6], line[7]]), 1)).reduceByKey(lambda a, b: a + b).map(
#     lambda countTuple: (countTuple[0][0], [countTuple[1]])).reduceByKey(lambda a, b: a + b).map(
#     lambda tuple: str(tuple[0], 'utf-8') + ',' + ','.join(str(x) for x in tuple[1]))

ip_count = myRdd.map(lambda line: line.split(',')).filter(lambda line: len(line) > 7).filter(
    lambda line: str(line[6]).startswith('147.32')).map(lambda line: tuple([line[3], line[6], line[7]])).distinct().map(
    lambda line: ((line[0], line[1]), 1)).reduceByKey(lambda a, b: a + b).map(lambda line: line[1])

ip_count.repartition(1).saveAsTextFile('ip_count')
