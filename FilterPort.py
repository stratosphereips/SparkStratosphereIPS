__author__ = 'username'

import datetime
from pyspark.context import SparkContext


ports = set()
ports.add('445')
ports.add('3389')

DATE_FORMAT = '%Y.%m.%d'
start = datetime.datetime.strptime('2017.04.15', DATE_FORMAT)
end = datetime.datetime.strptime('2017.05.14', DATE_FORMAT)
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]

filenames = []
hdfs_dir = 'hdfs:///user/ykasimov/CTU-Flows/'
for date in date_generated:
    str_date = date.strftime(DATE_FORMAT)
    file = hdfs_dir + str_date + '.binetflow'
    filenames.append(file)

sc = SparkContext()
myRdd = sc.textFile(','.join(filenames))

filtered_flows_by_port = myRdd.map(lambda line: line.split(',')).filter(lambda flow: flow[7] in ports).map(
    lambda line: ','.join(line))

filtered_flows_by_port.saveAsTextFile('flows_from_list')
