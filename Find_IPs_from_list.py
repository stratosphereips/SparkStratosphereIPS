__author__ = 'username'

import argparse
import sys
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

sc = SparkContext()
sq = SQLContext(sc)


def get_port_set(port_list):
    port_set = set()
    for port in port_list:
        if port:
            port_set.add(port)
    return port_set


parser = argparse.ArgumentParser(description='Spark Search')
parser.add_argument('-d', action='store', dest='hdfs_flows_directory')
parser.add_argument('-f', action='store', dest='ip_list_filename')
parser.add_argument('-from_date', action='store', dest='from_date')
parser.add_argument('-to_date', action='store', dest='to_date')
parser.add_argument('-p', nargs='*', dest='ports')
parameters = parser.parse_args(sys.argv[1:])

port_set = None
if parameters.ports is not None:
    port_set = get_port_set(parameters.ports)
hdfs_flows_directory = parameters.hdfs_flows_directory

ip_list_filename = parameters.ip_list_filename
_DATE_FORMAT = '%Y/%m/%d'
_DATE_FORMAT_FILE = '%Y-%m-%d'

from_date = '{d.year}-{d.month}-{d.day}'.format(d=datetime.strptime(parameters.from_date, _DATE_FORMAT))
to_date = '{d.year}-{d.month}-{d.day}'.format(d=datetime.strptime(parameters.to_date, _DATE_FORMAT))

search_ip = set()

if parameters.ip_list_filename:
    with open(ip_list_filename) as f:
        [search_ip.add(line.strip()) for line in list(f)]
search_ip = sc.broadcast(search_ip)

result = sq.read.parquet(hdfs_flows_directory).where((col('_yyyymd') >= from_date) & (col('_yyyymd') <= to_date))

if len(search_ip.value) > 0:
    result = result.where(
        (col('SrcAddr').isin(search_ip.value)) | (col('DstAddr').isin(search_ip.value)))

if port_set:
    result = result.where((col('Sport').isin(port_set)) | (col('Dport').isin(port_set)))

result.write.format(
    'com.databricks.spark.csv').save('flows_from_list')
