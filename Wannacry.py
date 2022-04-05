import sys
import os
from pyspark.context import SparkContext
import Constants
import datetime
from Common_functions import generate_filenames_date_range

ports = set()

ports.add('3389')
ports.add('445')

port445 = '445'
port3389 = '3389'

home_network = '147.'
filenames = generate_filenames_date_range('2017.04.01', '2017.05.14')
sc = SparkContext()
data = sc.textFile(','.join(filenames))

flows_filt_by_port445 = data.map(lambda line: line.split(',')).filter(lambda line: line[7] == port445)

flows_filt_by_port3389 = data.map(lambda line: line.split(',')).filter(lambda line: line[7] == port3389)

from_our_network_445 = flows_filt_by_port445.filter(lambda line: line[3].startswith(home_network))

from_our_network_3389 = flows_filt_by_port3389.filter(lambda line: line[3].startswith(home_network))

to_our_network_445 = flows_filt_by_port445.filter(lambda line: line[6].startswith(home_network))

to_our_network_3389 = flows_filt_by_port3389.filter(lambda line: line[6].startswith(home_network))

from_and_to_our_network_445 = from_our_network_445.filter(lambda line: line[6].startswith(home_network))

from_and_to_our_network_3389 = from_our_network_3389.filter(lambda line: line[6].startswith(home_network))

rdd_list = [from_our_network_445, from_our_network_3389, to_our_network_445, to_our_network_3389,
            from_and_to_our_network_445, from_and_to_our_network_3389]


def create_key_by_hour(line):
    line_date = datetime.datetime.strptime(line[0], Constants.FLOW_DATE_FORMAT)
    key = str(line_date.date()) + ' ' + str(line_date.hour)
    return key, 1


from_our_network_445 = from_our_network_445.map(create_key_by_hour).reduceByKey(lambda a, b: a + b)
from_our_network_3389 = from_our_network_3389.map(create_key_by_hour).reduceByKey(lambda a, b: a + b)
to_our_network_445 = to_our_network_445.map(create_key_by_hour).reduceByKey(lambda a, b: a + b)
to_our_network_3389 = to_our_network_3389.map(create_key_by_hour).reduceByKey(lambda a, b: a + b)
from_and_to_our_network_445 = from_and_to_our_network_445.map(create_key_by_hour).reduceByKey(lambda a, b: a + b)
from_and_to_our_network_3389 = from_and_to_our_network_3389.map(create_key_by_hour).reduceByKey(lambda a, b: a + b)

from_our_network_445.repartition(1).saveAsTextFile('from_out_network_445')
from_our_network_3389.repartition(1).saveAsTextFile('from_our_network_3389')
to_our_network_445.repartition(1).saveAsTextFile('to_our_network_445')
to_our_network_3389.repartition(1).saveAsTextFile('to_our_network_3389')
from_and_to_our_network_445.repartition(1).saveAsTextFile('from_and_to_our_network_445')
from_and_to_our_network_3389.repartition(1).saveAsTextFile('from_and_to_our_network_3389')
