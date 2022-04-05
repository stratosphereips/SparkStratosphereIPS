import sys
import os
from pyspark.context import SparkContext
import Constants
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf, hour, dayofmonth, month
from Common_functions import generate_filenames_date_range
from pyspark.sql import SQLContext

filenames = generate_filenames_date_range('2017.02.09', '2017.05.28')
port1 = 445
port2 = 3389
home_network = '147.%'


def readCSV(fname, sq):
    return sq.read.format('com.databricks.spark.csv') \
        .options(header='true', inferschema='true') \
        .load(','.join(fname))


sc = SparkContext()
sq = SQLContext(sc)
dataframe = readCSV(filenames, sq)

dataframe.registerTempTable('dataTable')

filtered_ports = sq.sql('SELECT * FROM dataTable WHERE Dport={} OR Dport={}'.format(port1, port2))

filtered_ports.registerTempTable('filtered_ports')
# to_our_network = sq.sql('SELECT * FROM filtered_ports WHERE DstAddr')



# user defined function to convert string to TimestampType TODO add it to the Common_functions.py file
convert_to_datetime = udf(lambda string_dage: datetime.strptime(string_dage, Constants.FLOW_DATE_FORMAT), TimestampType())

filtered_ports = filtered_ports.withColumn('StartTime_new', convert_to_datetime(col('StartTime')))

# to_our_network = filtered_ports.filter(filtered_ports.DstAddr.like(home_network))
from_our_network = filtered_ports.filter(filtered_ports.SrcAddr.like(home_network))
# to_from_our_network = to_our_network.filter(to_our_network.SrcAddr.like(home_network))

#####################################
# FAILED CONNECTIONS
#
# failed_connections_to_network = to_our_network.filter(
#     to_our_network.State.like('S_') | to_our_network.State.like('S_R'))
#
# failed_connections_to_network_445 = failed_connections_to_network.filter(
#     (failed_connections_to_network.Dport == '445') & (failed_connections_to_network.SrcPkts == '1'))
#
# hist = failed_connections_to_network_445.groupBy(month(failed_connections_to_network_445.StartTime_new).alias('month'),
#                                                  dayofmonth(failed_connections_to_network_445.StartTime_new).alias(
#                                                      'day'),
#                                                  hour(failed_connections_to_network_445.StartTime_new).alias(
#                                                      'hour')).count()
#
######################################

# ESTABLISHED CONNECTIONS
# established_connections_to_network = to_our_network.filter(
#     ~(to_our_network.State.like('S_') | to_our_network.State.like('S_R')))
#
# hist = established_connections_to_network.groupBy(
#     month(established_connections_to_network.StartTime_new).alias('month'),
#     dayofmonth(established_connections_to_network.StartTime_new).alias(
#         'day'),
#     hour(established_connections_to_network.StartTime_new).alias(
#         'hour')).count()

########################################################


# FAILED CONNECTIONS FROM OUR NETWORK

# failed_connections_from_network = from_our_network.filter(
#     from_our_network.State.like('S_') | from_our_network.State.like('S_R'))
#
# failed_connections_from_network_445 = failed_connections_from_network.filter(
#     (failed_connections_from_network.Dport == '445') & (failed_connections_from_network.SrcPkts == '1'))
#
# hist = failed_connections_from_network_445.groupBy(month(failed_connections_from_network_445.StartTime_new).alias('month'),
#                                                    dayofmonth(failed_connections_from_network_445.StartTime_new).alias(
#                                                      'day'),
#                                                    hour(failed_connections_from_network_445.StartTime_new).alias(
#                                                      'hour')).count()

#############################################################

# FAILED CONNECTIONS FROM AND TO OUR NETWORK

# failed_connections_from_to_network = to_from_our_network.filter(
#     to_from_our_network.State.like('S_') | to_from_our_network.State.like('S_R'))
#
# failed_connections_from_to_network_445 = failed_connections_from_to_network.filter(
#     (failed_connections_from_to_network.Dport == '445') & (failed_connections_from_to_network.SrcPkts == '1'))
#
# hist = failed_connections_from_to_network_445.groupBy(month(failed_connections_from_to_network_445.StartTime_new).alias('month'),
#                                                       dayofmonth(failed_connections_from_to_network_445.StartTime_new).alias(
#                                                      'day'),
#                                                       hour(failed_connections_from_to_network_445.StartTime_new).alias(
#                                                      'hour')).count()

#################################################################3
# Established connections from and to our network

# established_connections_to_network = from_our_network.filter(
#     ~(from_our_network.State.like('S_') | from_our_network.State.like('S_R')) & (
#         from_our_network.TotBytes > '28000'))
#
# hist = established_connections_to_network.groupBy(
#     month(established_connections_to_network.StartTime_new).alias('month'),
#     dayofmonth(established_connections_to_network.StartTime_new).alias(
#         'day'),
#     hour(established_connections_to_network.StartTime_new).alias(
#         'hour')).count()
##################################################################

# Established connection from our network
established_connections_from_network = from_our_network.filter(
    ~(from_our_network.State.like('S_') | from_our_network.State.like('S_R')))

hist = established_connections_from_network.groupBy(
    month(established_connections_from_network.StartTime_new).alias('month'),
    dayofmonth(established_connections_from_network.StartTime_new).alias(
        'day'),
    hour(established_connections_from_network.StartTime_new).alias(
        'hour')).count()

#####################################################################


hist = hist.coalesce(1)
hist.orderBy('month', 'day', 'hour').write.format(
    'com.databricks.spark.csv').save('hist')
# hist.write.format('com.databricks.spark.csv').save('hist')
