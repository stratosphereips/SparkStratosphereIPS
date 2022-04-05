__author__ = 'username'

from pyspark.context import SparkContext

sc = SparkContext()

myRdd = sc.textFile('hdfs:///user/ykasimov/CTU-Flows/*')

broken_files = myRdd.map(lambda line: line.split(',')).filter(lambda splitted: len(splitted) < 7).map(
    lambda filterd: filterd[0])

broken_files.saveAsTextFile('broken_files')
