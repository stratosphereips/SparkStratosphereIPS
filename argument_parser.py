import argparse
from datetime import datetime

_DATE_FORMAT = '%Y/%m/%d'

def valid_date(s):
    try:
        datetime.strptime(s, _DATE_FORMAT)
        return s
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


parser = argparse.ArgumentParser(description='Run Spark magic')

parser.add_argument('-ip', nargs='*', help='list of IPs to search')
parser.add_argument('-c', dest='shouldCopyFilesBack', action='store_false', default=True,
                    help='do NOT copy result files back to local computer')
parser.add_argument('-gr', dest='generateReport', action='store_true', default=False, help='not implemented')
parser.add_argument('-sf', dest='spark_file', default='Find_IPs_from_list.py', help='not implemented')
parser.add_argument('-add', dest='addIPsToIoC', action='store_true', default=False,
                    help='add provided IPs to the IoC file')
parser.add_argument('-ioc', dest='searchIoC', action='store_true', default=False, help='search IoC file')

parser.add_argument('-from_date', dest='from_date', type=valid_date,
                    default='1946/02/15',
                    help='The start date for searching -format YYYY/MM/DD. Default: 1946/02/15')

parser.add_argument('-to_date', dest='to_date', type=valid_date, default=datetime.today().strftime(_DATE_FORMAT),
                    help='The end date for searching -format YYYY/MM/DD. Default: current date')

parser.add_argument('-decrypt', dest='decrypt', action='store_true', default=False, help='decrypt files on the edge '
                                                                                         'node before copying them '
                                                                                         'back')

parser.add_argument('-p', dest='ports', nargs='*', default=None, help='list of ports to search')
parser.add_argument('--debug', dest='debug', action='store_true', default=False, help='enable prints for debug')
