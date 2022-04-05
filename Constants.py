__author__ = 'username'

HOMEDIR = '/spark_scripts/'

HDFS_RESULT_DIR = 'flows_from_list'

temporary_ip_list = 'tmp_list.txt'

DEFAULT_IP_LIST = 'IPlist.txt'

RESULT_DIR = 'Results/'

RESOURCE_DIR = 'Resources/'

PYTHON_CODE_DIR = HOMEDIR + 'PythonCode/'

FILES_TO_UPLOAD = ['Constants.py', 'Remote_Utils.py', 'Find_IPs_from_list.py', 'FilterPort.py']

HDFS_DIR_tmp = 'hdfs:///user/stratosphere/CTU-Flows_tmp/'

HDFS_DIR = 'hdfs:///user/stratosphere/CTU-Flows_main/Flows.parquet'

FILE_DATE_FORMAT = '%Y.%m.%d.%H'

FLOW_DATE_FORMAT = '%Y/%m/%d %H:%M:%S.%f'

IOC_FILE = 'IoC.txt'

SPARK_SUBMIT = 'spark-submit --master yarn --deploy-mode cluster --executor-cores 4 --packages ' \
               'com.databricks:spark-csv_2.10:1.5.0 --executor-memory 55G {0[resource_ip_file]} ' \
               '--num-executors 24 ' \
               '{0[spark_file]} ' \
               '{0[hdfs_directory]} ' \
               '{0[ip_file]} ' \
               '{0[from_date]} ' \
               '{0[to_date]}' \
               '{0[ports]}'

CD_TO_PYTHON_DIR = 'cd {};'.format(PYTHON_CODE_DIR)

USERNAME_FILE = '~/.spark_tool'
