# -*- coding: utf-8 -*-
import codecs
import base64
import os
packages = "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages {0} pyspark-shell".format(packages)
)
#os.environ["PYTHONIOENCODING"] = "utf-8";

from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.session import SparkSession

import json
#from pyspark.streaming.kafka import utf8_decoder

#from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
#from confluent_kafka.avro.serializer.message_serializer import MessageSerializer


conf = SparkConf()
conf.setMaster('spark://192.168.56.8:7077')
conf.setAppName('Demo Nhom 9')
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ss=SparkSession(sc)
ssc = StreamingContext(sc, 5)


KAFKA_BROKER = "192.168.56.9:9092"
KAFKA_TOPIC = "MyTopicDemo"
# Create a schema for the dataframe
schema = StructType([
    StructField('MSSV', StringType(), True),
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
])

print("contexts ===================")
#KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
kafkaStream = KafkaUtils.createDirectStream(ssc,[KAFKA_TOPIC],{"metadata.broker.list":KAFKA_BROKER})

#lines=kafkaStream.map(lambda value: json.loads(value[1]))
lines=kafkaStream.map(lambda value:value[1])

#coords=lines.map(lambda value:process(value.decode('base64')))
#coords=lines.map(lambda value:process(base64.standard_b64decode(value).decode('utf-8')))
#coords=lines.map(lambda value:(value,type(value)))
coords=lines.map(lambda value:str(base64.standard_b64decode(value)).split(","))
def handler_rdd(rdd):
        #rdd.foreach(lambda rec:codecs.open("output.txt", "a",encoding="utf8").write(str(rec) + "\n"))
        #rdd.coalesce(1).saveAsTextFile('out.txt')
        if not rdd.isEmpty():
                global ss
                df = ss.createDataFrame(rdd, schema)
                df.write.format('csv') .mode('append').option("header", "true").save("hdfs://192.168.56.10:9000/user/output/data3")
                df.show()

def empty_rdd():
        print("wait..")
coords.foreachRDD(lambda rdd: empty_rdd() if rdd.count() == 0 else handler_rdd(rdd))
coords.pprint()


ssc.start()
ssc.awaitTermination()