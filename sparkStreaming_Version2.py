# -*- coding: utf-8 -*-
import codecs
import base64
import os
import re
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.session import SparkSession

packages = "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages {0} pyspark-shell".format(packages)
)

conf = SparkConf()
conf.setMaster('spark://192.168.56.8:7077')
conf.setAppName('Demo Nhom 9')
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ss=SparkSession(sc)
ssc = StreamingContext(sc, 5)


KAFKA_BROKER = "192.168.56.9:9092"
KAFKA_TOPIC = "MyTopicDemo"

print("contexts =================== {} {}")

# Create a schema for the dataframe
schema = StructType([
    StructField('SBD', StringType(), True),
    StructField('NAME', StringType(), True),
    StructField('DATE', StringType(), True),
    StructField('TOAN', StringType(),True),
    StructField('VAN', StringType(),True),
    StructField('LI', StringType(),True),
    StructField('HOA', StringType(),True),
    StructField('SINH', StringType(),True),
    StructField('KHTN', StringType(),True),
    StructField('SU', StringType(),True),
    StructField('DIA', StringType(),True),
    StructField('GDCD', StringType(),True),
    StructField('KHXH', StringType(),True),
    StructField('ENG', StringType(),True),
    StructField('JAN', StringType(),True),
    StructField('FREN', StringType(),True),
    StructField('UNKNOWN', StringType(),True),
])


kafkaStream = KafkaUtils.createDirectStream(ssc,[KAFKA_TOPIC],{"metadata.broker.list":KAFKA_BROKER})
lines=kafkaStream.map(lambda value:value[1])

def handle_rdd(raw_data):
    raw_data = re.sub("[\t\n\r\f\v]", ' ', raw_data)
    raw_data = raw_data.replace("   ", " ")
    raw_data = raw_data.replace("   ", " ")
    raw_data = raw_data.replace("   ", " ")
    raw_data = raw_data.replace("   ", " ")
    raw_data = raw_data.replace("  ", " ")
    raw_data = raw_data.replace("  ", " ")

    ms = raw_data[len(raw_data) - 8:len(raw_data)]
    raw_data = raw_data[:len(raw_data) - 8]
    raw_data = raw_data[raw_data.find("Diem thi ") + len("Diem thi "):len(raw_data)]
    match = re.search("([0-9]{2}\/[0-9]{2}\/[0-9]{4})", raw_data)
    date = match.group()

    name = raw_data[:raw_data.find(date) - 1]
    raw_data = raw_data[raw_data.find(date) + len(date) + 1:len(raw_data)]
    # print(raw_data)
    points = re.findall('[-+]?(\d+([.,]\d*)?|[.,]\d+)([eE][-+]?\d+)?', raw_data)
    # print(points)
    points = [x[0] for x in points]
    # print(points)
    raw_data = re.sub(': [-+]?(\d+([.,]\d*)?|[.,]\d+)([eE][-+]?\d+)? ', ',', raw_data)
    raw_data = raw_data.split(',')
    # print(raw_data)
    result = []
    result.extend([ms, name, date])
    # toán,van,ly,hoa,sinh,khtn,su,dia,gdcd,khxh,eng,jan,fren,unKnown
    result.extend([-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1])
    raw_data.remove('')
    j = 0
    for subject in raw_data:
        point = float(points[j])
        if subject == 'Toan':
            result[3] = point
        elif subject == 'Ngu van':
            result[4] = point
        elif subject == 'Vat li':
            result[5] = point
        elif subject == 'Hoa hoc':
            result[6] = point
        elif subject == 'Sinh hoc':
            result[7] = point
        elif subject == 'KHTN':
            result[8] = point
        elif subject == 'Lich su':
            result[9] = point
        elif subject == 'Dia li':
            result[10] = point
        elif subject == 'GDCD':
            result[11] = point
        elif subject == 'KHXH':
            result[12] = point
        elif subject == 'Tieng Anh':
            result[13] = point
        elif subject == 'Tieng Nhat':
            result[14] = point
        elif subject == 'Tieng Phap':
            result[15] = point
        else:
            result[16] = point
        j = j + 1
    return result


coords=lines.map(lambda value:handle_rdd(value))
def save_rdd(rdd):
	if not rdd.isEmpty():
	        global ss
        	df = ss.createDataFrame(rdd, schema)
		df.write.format('csv') .mode('append').option("header", "true").save("hdfs://192.168.56.10:9000/user/output/data11")
		df.show()

def empty_rdd():
    print("....")
coords.foreachRDD(lambda rdd: empty_rdd() if rdd.count() == 0 else save_rdd(rdd))

coords.pprint()

ssc.start()
ssc.awaitTermination()
