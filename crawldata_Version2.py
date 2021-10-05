import subprocess
from bs4 import BeautifulSoup
import csv
import base64
from kafka import KafkaProducer
from datetime import datetime
import sys
import json
from bson import json_util
import time
import re
import requests
import htmlentities

import unidecode

KAFKA_BROKER = '192.168.56.9:9092'
KAFKA_TOPIC = 'MyTopicDemo'
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
SBD='02000000'
i=1
while True:
    if i > 74719:
        break
    SBD = SBD[:len(SBD) - len(str(i))] + str(i)
    print(SBD)
    headers = {'User-Agent': 'Mozilla/5.0'}
    html = requests.post('http://diemthi.hcm.edu.vn/Home/Show', headers=headers, data={'SoBaoDanh': SBD})
    soup=BeautifulSoup(html.text, "html.parser").text
    raw_data = unidecode.unidecode(soup + SBD)
    if raw_data.find('Khong tim thay so bao danh nay !') != -1:
        i=i+1;
        continue;
    #Gửi dữ liệu lên kafka
    producer.send(KAFKA_TOPIC, bytes(unidecode.unidecode(raw_data), 'utf-8'))
    i = i + 1



