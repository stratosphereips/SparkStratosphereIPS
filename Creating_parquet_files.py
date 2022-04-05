from struct import pack
from Crypto import Random
from Crypto.Cipher import Blowfish
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
import Common_functions as cf
import base64
import string
from os.path import basename

filenames = cf.generate_filenames_date_range('2018.04.10.00', '2018.04.25.23')
sc = SparkContext()
sq = SQLContext(sc)

bs = Blowfish.block_size
iv = Random.new().read(bs)
key = b'(,}65dFep_hZJ7)v-^EYhG9a(5DEp<W-!^99?7_Y%U8kp(sZrF&:#L'


def decode_src_data(line):
    data = line[16]
    try:
        index = data.find('=') + 1
    except AttributeError:
        return ''
    output = ''
    try:
        for charachter in base64.b64decode(data[index:]):
            if charachter not in string.printable or hex(ord(charachter)) == '0xa' or hex(ord(charachter)) == '0xd':
                output += '.'
            else:
                output += charachter
        line[16] = str(data[0:index] + output)
        return line
    except (IndexError, TypeError):
        # Probably not base64, so just return the string
        return line


def decode_dst_data(line):
    data = line[17]
    try:
        index = data.find('=') + 1
    except AttributeError:
        return ''
    output = ''
    try:
        for charachter in base64.b64decode(data[index:]):
            if charachter not in string.printable or hex(ord(charachter)) == '0xa' or hex(ord(charachter)) == '0xd':
                output += '.'
            else:
                output += charachter
        line[17] = str(data[0:index] + output)
        return line
    except (IndexError, TypeError):
        # Probably not base64, so just return the string
        return line


def encrypt_data(data, iv, key, bs):
    plen = bs - divmod(len(data), bs)[1]
    padding = [plen] * plen
    padding = pack('b' * plen, *padding)
    return base64.b64encode(iv + Blowfish.new(key, Blowfish.MODE_CBC, iv).encrypt(data + padding))


def decrypt_data(encrypted_data):
    encrypted_data = base64.b64decode(encrypted_data)
    iv = encrypted_data[:bs]
    encrypted_data = encrypted_data[bs:]
    cipher = Blowfish.new(key, Blowfish.MODE_CBC, iv)
    decrypted = cipher.decrypt(encrypted_data)
    last_byte = decrypted[-1]
    decrypted = decrypted[:- (last_byte if type(last_byte) is int else ord(last_byte))]
    return decrypted


encrypt_udf = UserDefinedFunction(lambda data: encrypt_data(data, iv, key, bs), StringType())

# decrypt_udf = UserDefinedFunction(lambda data: decrypt_data(data), StringType())
header = [u'StartTime', u'Dur', u'Proto', u'SrcAddr', u'Sport', u'Dir', u'DstAddr', u'Dport', u'State', u'sTos', u'dTos',
 u'TotPkts', u'TotBytes', u'SrcBytes', u'SrcPkts', u'Label', u'srcUdata', u'dstUdata']
field = [StructField(name, StringType(), True) for name in header]
schema = StructType(field)


for file in filenames:
    iv = Random.new().read(bs)
    cipher = Blowfish.new(key, Blowfish.MODE_CBC, iv)
    data = sc.textFile(file)
    decoded_from_base64 = data.map(lambda line: line.split(',')).filter(lambda row: row != header).filter(lambda row : len(row)==len(header)).map(
        lambda line: decode_src_data(line)).map(lambda line: decode_dst_data(line))
    if decoded_from_base64.isEmpty():
        df = sq.createDataFrame(sc.emptyRDD(), schema)
    else:
        df = decoded_from_base64.toDF(header)
    column_name_src = 'srcUdata'
    column_name_dst = 'dstUdata'
    encrypted_df = df.select(
        *[encrypt_udf(column).alias(column) if ((column == column_name_src) | (column == column_name_dst)) else column
          for column in df.columns])
    filename = basename(file)
    encrypted_df.write.parquet('hdfs:///user/stratosphere/CTU-Flows_encrypted/' + filename + '.parquet')

    # df.write.parquet(file + '.parquet')


    # df.select(*[decrypt_udf(column).alias(column) if ((column == column_name)|(column == 'dstUdata')) else column for column in df.columns])
