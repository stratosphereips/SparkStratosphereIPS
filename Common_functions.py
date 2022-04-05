import datetime
import Constants



def generate_filenames_date_range(date_from, date_to):
    start = datetime.datetime.strptime(date_from, Constants.FILE_DATE_FORMAT)
    end = datetime.datetime.strptime(date_to, Constants.FILE_DATE_FORMAT)
    date_generated = [start + datetime.timedelta(hours=x) for x in
                      range(0, (end - start).days * 24 + (end.hour - start.hour) + 1)]
    filenames = []
    for date in date_generated:
        str_date = date.strftime(Constants.FILE_DATE_FORMAT)
        file = Constants.HDFS_DIR_tmp + str_date + '.binetflow'
        filenames.append(file)
    return filenames


def readCSV(fname, sq):
    return sq.read.format('com.databricks.spark.csv') \
        .options(header='true', inferschema='true') \
        .option("parserLib", "UNIVOCITY") \
        .option('sep', ',') \
        .option("maxCharsPerCol", "1100000") \
        .load(fname, rowSeparator='\n')  # , rowSeparator='\r\n')


