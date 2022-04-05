from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import dayofmonth, month, year, col
from pyspark.sql.types import TimestampType
from datetime import datetime

sc = SparkContext()
sq = SQLContext(sc)
sc._jsc.hadoopConfiguration().set('spark.sql.parquet.compression.codec', "snappy")
sc._jsc.hadoopConfiguration().set('parquet.enable.summary-metadata', 'false')



df = sq.read.parquet('CTU-Flows_encrypted/*')

df2 = df.withColumn('_StartTime', unix_timestamp(df['StartTime'], 'yyyy/MM/dd HH:mm:ss').cast(TimestampType()))
df_with_date = df2.withColumn('_year', year(col('_StartTime'))).withColumn('_month', month(col(
    '_StartTime'))).withColumn('_day', dayofmonth(col('_StartTime')))

df_with_date = df_with_date.repartition('_year',
                                        '_month', '_day')

df_with_date.write.option('compression', 'snappy').partitionBy('_year', '_month', '_day').mode(
    'append').parquet('hdfs:///user/stratosphere/CTU-Flows_main/' + 'Flows' + '.parquet')



