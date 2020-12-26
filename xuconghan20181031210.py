from __future__ import print_function

import paho.mqtt.client as mqtt
import struct
import json
import urllib.request
import urllib.error
import datetime
import serial
import re
import time
import sqlite3
# 导入串口包
# CONNECT 方式：
# client_id:     DEV_ID
# username:  PRO_ID
# password:   AUTHINFO(鉴权信息)
# 可以连接上设备云，CONNECT 和 CONNACK握手成功
# temperature:已创建的一个数据流
#更多请查阅OneNet官方mqtt文档与paho-mqtt开发文档







#修改成自己的即可
DEV_ID = "635699370" #设备ID
PRO_ID = "375540" #产品ID
AUTH_INFO = "xKuEwG4tHc=by7SMCvOmIz=83V8="  #APIKEY


TYPE_JSON = 0x01
TYPE_FLOAT = 0x17

#定义上传数据的json格式  该格式是oneNET规定好的  按格式修改其中变量即可
#while 1:
def get():
    time2=datetime.datetime.now().isoformat()
    ser =serial.Serial("/dev/" + "ttyUSB0", baudrate=115200, timeout=2)    
    data1= ser.readline()
    data2 = data1.decode('UTF-8')
    data2=re.findall(r"\d+\.?\d*",data2)
    data3=data2[0]
    data3=str(data3)
    print(data3)
    
    conn=sqlite3.connect('/home/pi/txt/1223/1223.db')
    cursor=conn.cursor()
    cursor.execute("INSERT INTO temperature(time0,tem)values ('{}','{}')".format(time2,data3))
    cursor.close
    conn.commit()
    # 关闭Connection:
    #conn.close()
    print(type(conn))


    
    
    con=sqlite3.connect('/home/pi/txt/1223/1223.db')
    c = con.cursor()
    sql_str="SELECT * FROM temperature;"
    c.execute(sql_str)
    for row in c:
        data6=list(row)
    data5=data6[1]
    con.close()


    


    return data5


    # 定义上传数据的json格式  该格式是oneNET规定好的  按格式修改其中变量即可




def build_payload(type, payload):
    datatype = type
    packet = bytearray()
    packet.extend(struct.pack("!B", datatype))
    if isinstance(payload, str):
        udata = payload.encode('utf-8')
        length = len(udata)
        packet.extend(struct.pack("!H" + str(length) + "s", length, udata))
    return packet


# 当客户端收到来自服务器的CONNACK响应时的回调。也就是申请连接，服务器返回结果是否成功等
def on_connect(client, userdata, flags, rc):
    times=datetime.datetime.now().isoformat()
    data7=get()
    datas=float(data7)
    print("连接结果:" + mqtt.connack_string(rc))
    #上传数据
    time.sleep(3)
    body = {
        "datastreams": [
            {
                "id": "tem",  # 对应OneNet的数据流名称
                "datapoints": [
                    {
                        "at": times,  # 数据提交时间，这里可通过函数来获取实时时间
                        "value": datas  # 数据值
                    }
                ]
            }
        ]
    }
    
    json_body = json.dumps(body)
    packet = build_payload(TYPE_JSON, json_body)
    
    client.publish("$dp", packet, qos=1)  #qos代表服务质量


# 从服务器接收发布消息时的回调。
def on_message(client, userdata, msg):
    print("温度:"+str(msg.payload,'utf-8')+"°C")


#当消息已经被发送给中间人，on_publish()回调将会被触发
def on_publish(client, userdata, mid):
    print("mid:" + str(mid))
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_message = on_message

    client.username_pw_set(username=PRO_ID, password=AUTH_INFO)
    client.connect('183.230.40.39', port=6002, keepalive=120)
    

def main():

   

    client = mqtt.Client(client_id=DEV_ID, protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_message = on_message

    client.username_pw_set(username=PRO_ID, password=AUTH_INFO)
    client.connect('183.230.40.39', port=6002, keepalive=120)
    
    client.loop_forever()
        
if __name__ == '__main__':
        main()
