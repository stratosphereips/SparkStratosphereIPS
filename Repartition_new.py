from pyspark.context import SparkContext
from pyspark.sql.functions import dayofmonth, month, year, col
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import TimestampType

sc = SparkContext()
sq = SQLContext(sc)
df = sq.read.parquet('CTU-Flows_encrypted/*')

df2 = df.withColumn('_StartTime', unix_timestamp(df['StartTime'], 'yyyy/MM/dd HH:mm:ss').cast(TimestampType()))
df_with_date = df2.withColumn('_year', year(col('_StartTime'))).withColumn('_month',
                                                                           month(col('_StartTime'))).withColumn('_day',
                                                                                                                dayofmonth(
                                                                                                                    col(
                                                                                                                        '_StartTime')))
df_with_new_col = df_with_date.withColumn('_yyyymd',
                                          concat(col('_year'), lit('-'), col('_month'), lit('-'), col('_day')))

df_repart = df_with_new_col.drop('_StartTime').drop('_year').drop('_day').drop('_month').repartition('_yyyymd')

df_repart.write.option('compression', 'snappy').partitionBy('_yyyymd').mode('append').parquet(
    'hdfs:///user/stratosphere/CTU-Flows_main/Flows.parquet')
