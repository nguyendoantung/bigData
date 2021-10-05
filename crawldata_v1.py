import subprocess
from bs4 import BeautifulSoup
import csv
import time
import base64
# import json
from kafka import KafkaProducer
from datetime import datetime
import sys
import unidecode
csv_file = open('ttdl_temp.csv', 'w', encoding="utf-8")
csv_writer = csv.writer(csv_file)
csv_writer.writerow(['name', 'date', 'toan', 'van' , 'li', 'hoa', 'sinh' , 'KHTN', 'su', 'dia', 'GDCD', 'KHXH' , 'eng', 'nhat'])
KAFKA_BROKER = '192.168.56.9:9092'
KAFKA_TOPIC = 'MyTopicDemo'


try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
except Exception as e:
    print(f'Error Connecting to Kafka --> {e}')
    sys.exit(1)

with open('sbd2.txt') as f:
    lines = f.readlines()
with open("sbd2.txt", "r") as ins:
    array = []
    for line in ins:
        array.append(line)
        line1 = line[0: len(line)-1]

        link = 'curl -F "SoBaoDanh=' + line1 + '" diemthi.hcm.edu.vn/Home/Show'
        link.encode()
        res = subprocess.check_output(link, shell=True)
        html = res.decode()
        #print(type((res.decode() + str(line1)).encode('utf-8')))
        # data = data.replace()
        soup = BeautifulSoup(html, "html.parser")
        index = 1
        ja1 = ''
        mssv=line1
        name1 = date1 = toan1 = van1 = li1 = hoa1 = sinh1 = KHTN1 = su1 = dia1 = GDCD1 = KHXH1 = eng1 = ja1
        for item in soup.select("td"):
            if index > 3:
                if index == 4:
                    name = item.get_text()
                    name = name.lstrip()
                    name1 = name[0:len(name) - 14]
                if index == 5:
                    date = item.get_text()
                    date = date.lstrip()
                    date1 = date[0:len(date) - 14]
                if index == 6:
                    data = item.get_text()
                    data = data.lstrip()
                    data1 = data[0:len(data) - 17]
                    raw_data2 = data1
                    toan = van = li = hoa = sinh = KHTN = su = dia = GDCD = KHXH = eng = ja = fre = -1
                    toan1 = van1 = li1 = hoa1 = sinh1 = KHTN1 = su1 = dia1 = GDCD1 = KHXH1 = eng1 = ja1 = fre1 = -1
                    toan2 = van2 = li2 = hoa2 = sinh2 = KHTN2 = su2 = dia2 = GDCD2 = KHXH2 = eng2 = ja2 = fre2 = -1
                    if raw_data2.find("Toán:") != -1:
                        toan = raw_data2[
                               raw_data2.find("Toán:") + len("Toán:   "): raw_data2.find("Toán:") + len("Toán:   ") + 4]
                        toan1 = toan.replace(" ", "")
                        try:
                            toan2 = float(toan1)
                        except:
                            toan2 = -1
                    if raw_data2.find("Ngữ văn:") != -1:
                        van = raw_data2[
                              raw_data2.find("Ngữ văn:") + len("Ngữ văn:   "): raw_data2.find("Ngữ văn:") + len(
                                  "Ngữ văn:   ") + 4]
                        van1 = van.replace(" ", "")
                        try:
                            van2 = float(van1)
                        except:
                            van2 = -1
                        # print(van1)
                    if raw_data2.find("Vật lí:") != -1:
                        li = raw_data2[raw_data2.find("Vật lí") + len("Vật lí:   "): raw_data2.find("Vật lí:") + len(
                            "Vật lí:   ") + 4]
                        li1 = li.replace(" ", "")
                        try:
                            li2 = float(li1)
                        except:
                            li2 = -1
                        # print(li1)
                    if raw_data2.find("Hóa học:") != -1:
                        hoa = raw_data2[
                              raw_data2.find("Hóa học:") + len("Hóa học:   "): raw_data2.find("Hóa học:") + len(
                                  "Hóa học:   ") + 4]
                        hoa1 = hoa.replace(" ", "")
                        try:
                            hoa2 = float(hoa1)
                        except:
                            hoa2 = -1
                        # print(hoa1)
                    if raw_data2.find("Sinh học:") != -1:
                        sinh = raw_data2[
                               raw_data2.find("Sinh học:") + len("Sinh học:   "): raw_data2.find("Sinh học:") + len(
                                   "Sinh học:   ") + 4]
                        sinh1 = sinh.replace(" ", "")
                        try:
                            sinh2 = float(sinh1)
                        except:
                            sinh2 = -1
                        # print(sinh1)
                    if raw_data2.find("KHTN:") != -1:
                        KHTN = raw_data2[
                               raw_data2.find("KHTN:") + len("KHTN: "): raw_data2.find("KHTN:") + len("KHTN: ") + 4]
                        KHTN1 = KHTN.replace(" ", "")
                        try:
                            KHTN2 = float(KHTN1)
                        except:
                            KHTN2= -1
                        # print(KHTN1)
                    if raw_data2.find("Lịch sử:") != -1:
                        su = raw_data2[
                             raw_data2.find("Lịch sử:") + len("Lịch sử:   "): raw_data2.find("Lịch sử:") + len(
                                 "Lịch sử:   ") + 4]
                        su1 = su.replace(" ", "")
                        try:
                            su2 = float(su1)
                        except:
                            su2 = -1
                        # print(su1)
                    if raw_data2.find("Địalí:") != -1:
                        dia = raw_data2[raw_data2.find("Địa lí:") + len("Địa lí:   "): raw_data2.find("Địa lí:") + len(
                            "Địa lí:   ") + 4]
                        dia1 = dia.replace(" ", "")
                        try:
                            dia2 = float(dia1)
                        except:
                            dia2 = -1
                    if raw_data2.find("GDCD:") != -1:
                        GDCD = raw_data2[
                               raw_data2.find("GDCD:") + len("GDCD:   "): raw_data2.find("GDCD:") + len("GDCD:   ") + 4]
                        GDCD1 = GDCD.replace(" ", "")
                        try:
                            GDCD2 = float(GDCD1)
                        except:
                            GDCD2 = -1
                        # print(GDCD1)
                    if raw_data2.find("KHXH:") != -1:
                        KHXH = raw_data2[
                               raw_data2.find("KHXH:") + len("KHXH: "): raw_data2.find("KHXH:") + len("KHXH: ") + 4]
                        KHXH1 = KHXH.replace(" ", "")
                        try:
                            KHXH2 = float(KHXH1)
                        except:
                            KHXH2 = -1
                    if raw_data2.find("Tiếng Anh:") != -1:
                        eng = raw_data2[
                              raw_data2.find("Tiếng Anh:") + len("Tiếng Anh:   "): raw_data2.find("Tiếng Anh:") + len(
                                  "Tiếng Anh:   ") + 4]
                        eng1 = eng.replace(" ", "")
                        try:
                            eng2 = float(eng1)
                        except:
                            eng2 = -1
                        # print(eng1)
                    if raw_data2.find("Tiếng Nhật:") != -1:
                        ja = raw_data2[
                             raw_data2.find("Tiếng Nhật:") + len("Tiếng Nhật:   "): raw_data2.find("Tiếng Nhật:") + len(
                                 "Tiếng Nhật:   ") + 4]
                        ja1 = ja.replace(" ", "")
                        try:
                            ja2 = float(ja1)
                        except:
                            ja2 = -1
                        # print(ja1)
                    if raw_data2.find("Tiếng Pháp:") != -1:
                        fre = raw_data2[
                              raw_data2.find("Tiếnp:") + len("Tiếng Pháp:   "): raw_data2.find("Tiếng Pháp:") + len(
                                  "Tiếng Pháp:   ") + 4]
                        fre1 = fre.replace(" ", "")
                        try:
                            fre2 = float(fre2)
                        except:
                            fre1 = -1
            index += 1
        message=mssv+","+ unidecode.unidecode(name1)+","+ str(date1)+","+str(toan1)+","+str(van1)+","+str(li1)+","+str(hoa1)+","+str(sinh1)+","+str(KHTN1)+","+str(su1)+","+str(dia1)+","+str(GDCD1)+","+str(KHXH1)+","+str(eng1)+","+str(ja1)+","+str(fre1);
        print(message)
        message_bytes = message.encode('utf-8')
        encoded = base64.b64encode(message_bytes)
        producer.send(KAFKA_TOPIC, encoded)
        #csv_writer.writerow([name1, date1, toan1, van1, li1 , hoa1 , sinh1 ,KHTN1 , su1 , dia1 , GDCD1 , KHXH1 ,eng1, ja1, fre1])
        time.sleep(1)