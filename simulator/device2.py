"""
    cihaz belirli aralıkalrla 20 bilgi gonderecek
    alarm olustugunda alarm gonderecek
    cihaza islem yaptırılabilecek
"""
import datetime
import json

import uvicorn


import threading
import time

from fastapi import FastAPI, Request
from fastapi_mqtt import FastMQTT, MQTTConfig
from starlette.middleware.cors import CORSMiddleware

app = FastAPI()

mqtt_config = MQTTConfig(host = "broker.hivemq.com",
    port= 1883,
    keepalive = 60,
    username="",
    password="",
    version=4)


mqtt = FastMQTT(
    config=mqtt_config)

mqtt.init_app(app)





dev = {"DI1":0, "DI2":0, "DI3":0, "DI4":0, "DI5":0,
        "DO1":0, "DO2":0, "DO3":0, "DO4":0, "DO5":0,
        "AI1":0, "AI2":0, "AI3":0, "AI4":0, "AI5":0,
        "AO1":0, "AO2":0, "AO3":0, "AO4":0, "AO5":0
       }

class Device():
    def __init__(self, uid, slaveId=1, rutinRead=8, rutinSend=65):
        self.stop_all_thread = False
        self.UID = uid
        self.SLAVEID = slaveId
        self.rutinRead = rutinRead
        self.rutinSend = rutinSend

        self.subTopic="devices/"+uid+"/messages"
        self.pubTopic="LynxDevices"
        self.MSGID = 0
        self.IO = {
            'DI1': 0, 'DI2': 0, 'DI3': 0, 'DI4': 0, 'DI5': 0,
            'DO1': 1, 'DO2': 1, 'DO3': 0, 'DO4': 0, 'DO5': 0,
            'AI1': 0, 'AI2': 0, 'AI3': 0, 'AI4': 0, 'AI5': 0,
            'AO1': 0, 'AO2': 0, 'AO3': 0, 'AO4': 0, 'AO5': 1
        }
        self.alert = {
            "DI1": 1, "DI1MSG": "",
            "DI2": 1, "DI2MSG": "",
            "DI3": 0, "DI3MSG": "",
            "DI4": 0, "DI4MSG": "",
            "DI5": 0, "DI5MSG": "",
            "DO1": 1, "DO1MSG": "MOTOR DURUMU",
            "DO2": 1, "DO2MSG": "EK GUC DURUMU",
            "DO3": 0, "DO3MSG": "",
            "DO4": 0, "DO4MSG": "",
            "DO5": 0, "DO5MSG": "",
            "AI1": 1, "AI1MSG": "",
            "AI2": 0, "AI2MSG": "",
            "AI3": 0, "AI3MSG": "",
            "AI4": 0, "AI4MSG": "",
            "AI5": 0, "AI5MSG": "",
            "AO1": 0, "AO1MSG": "",
            "AO2": 0, "AO2MSG": "",
            "AO3": 0, "AO3MSG": "",
            "AO4": 0, "AO4MSG": "",
            "AO5": 1, "AO5MSG": "MOTOR CIHAZI CALISIR DURUMDA"
        }

    def RutinRead(self):
        while(1):
            try:
                msg = {}
                IO = {}
                alert = {}
                AL = []

                if (self.IO.get("DI1") != dev.get("DI1")):
                    self.IO["DI1"] = dev.get("DI1")
                    IO["DI1"] = self.IO.get("DI1")
                    if (self.alert.get("DI1", 0) == 1):
                        AL.append({"NAME": "DI1", "VALUE": self.IO.get("DI1"), "MSG": self.alert.get("DI1MSG")})

                if (self.IO.get("DI2") != dev.get("DI2")):
                    self.IO["DI2"] = dev.get("DI2")
                    IO["DI2"] = self.IO.get("DI2")
                    if (self.alert.get("DI2", 0) == 1):
                        AL.append({"NAME": "DI2", "VALUE": self.IO.get("DI2"), "MSG": self.alert.get("DI2MSG")})

                if (self.IO.get("DI3") != dev.get("DI3")):
                    self.IO["DI3"] = dev.get("DI3")
                    IO["DI3"] = self.IO.get("DI3")
                    if (self.alert.get("DI3", 0) == 1):
                        AL.append({"NAME": "DI3", "VALUE": self.IO.get("DI3"), "MSG": self.alert.get("DI3MSG")})

                if (self.IO.get("DI4") != dev.get("DI4")):
                    self.IO["DI4"] = dev.get("DI4")
                    IO["DI4"] = self.IO.get("DI4")
                    if (self.alert.get("DI1", 0) == 1):
                        AL.append({"NAME": "DI4", "VALUE": self.IO.get("DI4"), "MSG": self.alert.get("DI4MSG")})

                if (self.IO.get("DI5") != dev.get("DI5")):
                    self.IO["DI5"] = dev.get("DI5")
                    IO["DI5"] = self.IO.get("DI5")
                    if (self.alert.get("DI5", 0) == 1):
                        AL.append({"NAME": "DI5", "VALUE": self.IO.get("DI5"), "MSG": self.alert.get("DI5MSG")})

                if (self.IO.get("DO1") != dev.get("DO1")):
                    self.IO["DO1"] = dev.get("DO1")
                    IO["DO1"] = self.IO.get("DO1")
                    if (self.alert.get("DO1", 0) == 1):
                        AL.append({"NAME": "DO1", "VALUE": self.IO.get("DO1"), "MSG": self.alert.get("DO1MSG")})

                if (self.IO.get("DO2") != dev.get("DO2")):
                    self.IO["DO2"] = dev.get("DO2")
                    IO["DO2"] = self.IO.get("DO2")
                    if (self.alert.get("DO2", 0) == 1):
                        AL.append({"NAME": "DO2", "VALUE": self.IO.get("DO2"), "MSG": self.alert.get("DO2MSG")})

                if (self.IO.get("DO3") != dev.get("DO3")):
                    self.IO["DO3"] = dev.get("DO3")
                    IO["DO3"] = self.IO.get("DO3")
                    if (self.alert.get("DO3", 0) == 1):
                        AL.append({"NAME": "DO3", "VALUE": self.IO.get("DO3"), "MSG": self.alert.get("DO3MSG")})

                if (self.IO.get("DO4") != dev.get("DO4")):
                    self.IO["DO4"] = dev.get("DO4")
                    IO["DO4"] = self.IO.get("DO4")
                    if (self.alert.get("DO4", 0) == 1):
                        AL.append({"NAME": "DO4", "VALUE": self.IO.get("DO4"), "MSG": self.alert.get("DO4MSG")})

                if (self.IO.get("DO5") != dev.get("DO5")):
                    self.IO["DO5"] = dev.get("DO5")
                    IO["DO5"] = self.IO.get("DO5")
                    if (self.alert.get("DO5", 0) == 1):
                        AL.append({"NAME": "DO5", "VALUE": self.IO.get("DO5"), "MSG": self.alert.get("DO5MSG")})

                if (self.IO.get("AI1") != dev.get("AI1")):
                    self.IO["AI1"] = dev.get("AI1")
                    IO["AI1"] = self.IO.get("AI1")
                    if (self.alert.get("AI1", 0) == 1):
                        AL.append({"NAME": "AI1", "VALUE": self.IO.get("AI1"), "MSG": self.alert.get("AI1MSG")})

                if (self.IO.get("AI2") != dev.get("AI2")):
                    self.IO["AI2"] = dev.get("AI2")
                    IO["AI2"] = self.IO.get("AI2")
                    if (self.alert.get("AI2", 0) == 1):
                        AL.append({"NAME": "AI2", "VALUE": self.IO.get("AI2"), "MSG": self.alert.get("AI2MSG")})

                if (self.IO.get("AI3") != dev.get("AI3")):
                    self.IO["AI3"] = dev.get("AI3")
                    IO["AI3"] = self.IO.get("AI3")
                    if (self.alert.get("AI4", 0) == 1):
                        AL.append({"NAME": "AI4", "VALUE": self.IO.get("AI4"), "MSG": self.alert.get("AI4MSG")})

                if (self.IO.get("AI4") != dev.get("AI4")):
                    self.IO["AI4"] = dev.get("AI4")
                    IO["AI4"] = self.IO.get("AI4")
                    if (self.alert.get("AI4", 0) == 1):
                        AL.append({"NAME": "AI4", "VALUE": self.IO.get("AI4"), "MSG": self.alert.get("AI4MSG")})

                if (self.IO.get("AI5") != dev.get("AI5")):
                    self.IO["AI5"] = dev.get("AI5")
                    IO["AI5"] = self.IO.get("AI5")
                    if (self.alert.get("AI5", 0) == 1):
                        AL.append({"NAME": "AI5", "VALUE": self.IO.get("AI5"), "MSG": self.alert.get("AI5MSG")})

                if (self.IO.get("AO1") != dev.get("AO1")):
                    self.IO["AO1"] = dev.get("AO1")
                    IO["AO1"] = self.IO.get("AO1")
                    if (self.alert.get("AO1", 0) == 1):
                        AL.append({"NAME": "AO1", "VALUE": self.IO.get("AO1"), "MSG": self.alert.get("AO1MSG")})

                if (self.IO.get("AO2") != dev.get("AO2")):
                    self.IO["AO2"] = dev.get("AO2")
                    IO["AO2"] = self.IO.get("AO2")
                    if (self.alert.get("AO2", 0) == 1):
                        AL.append({"NAME": "AO2", "VALUE": self.IO.get("AO2"), "MSG": self.alert.get("AO2MSG")})

                if (self.IO.get("AO3") != dev.get("AO3")):
                    self.IO["AO3"] = dev.get("AO3")
                    IO["AO3"] = self.IO.get("AO3")
                    if (self.alert.get("AO3", 0) == 1):
                        AL.append({"NAME": "AO3", "VALUE": self.IO.get("AO3"), "MSG": self.alert.get("AO3MSG")})

                if (self.IO.get("AO4") != dev.get("AO4")):
                    self.IO["AO4"] = dev.get("AO4")
                    IO["AO4"] = self.IO.get("AO4")
                    if (self.alert.get("AO4", 0) == 1):
                        AL.append({"NAME": "AO4", "VALUE": self.IO.get("AO4"), "MSG": self.alert.get("AO4MSG")})

                if (self.IO.get("AO5") != dev.get("AO5")):
                    self.IO["AO5"] = dev.get("AO5")
                    IO["AO5"] = self.IO.get("AO5")
                    if (self.alert.get("AO5", 0) == 1):
                        AL.append({"NAME": "AO5", "VALUE": self.IO.get("AO5"), "MSG": self.alert.get("AO5MSG")})


                if(len(IO)>0):
                    self.MSGID +=1
                    timestamp = str(datetime.datetime.now())
                    msg["timestamp"] = timestamp
                    msg["UID"] = self.UID
                    msg["MSGID"] = self.MSGID
                    msg["SLAVEID"] = self.SLAVEID
                    msg["IO"] = IO
                    # mqtt.publish(self.pubTopic, json.dumps({"RUTIN":msg}))
                    mqtt.client.publish(message_or_topic=self.pubTopic, payload=json.dumps({"RUTIN":msg}))
                if(len(AL)>0):
                    self.MSGID +=1
                    timestamp = str(datetime.datetime.now())
                    alert["timestamp"] = timestamp
                    alert["UID"] = self.UID
                    alert["MSGID"] = self.MSGID
                    alert["SLAVEID"] = self.SLAVEID
                    alert["AL"] = AL
                    # mqtt.publish(self.pubTopic, json.dumps({"ALERT":alert}))
                    mqtt.client.publish(message_or_topic=self.pubTopic, payload=json.dumps({"ALERT":alert}))
            except:
                print("rutin read error")

            t1 = time.time()
            while (time.time() - t1 < self.rutinRead):
                time.sleep(1)
                if (self.stop_all_thread == True):
                    break
            if (self.stop_all_thread == True):
                print("stop RutinRead thread")
                break

    def RutinSend(self):
        while (1):
            try:
                timestamp = str(datetime.datetime.now())
                msg = {"timestamp": timestamp, "UID":self.UID, "MSGID":self.MSGID, "SLAVEID":self.SLAVEID,
                        "IO": {
                                "DI1":self.IO.get("DI1"), "DI2":self.IO.get("DI2"), "DI3":self.IO.get("DI3"),
                                "DI4":self.IO.get("DI4"), "DI5":self.IO.get("DI5"),
                                "DO1":self.IO.get("DO1"), "DO2":self.IO.get("DO2"), "DO3":self.IO.get("DO3"),
                                "DO4":self.IO.get("DO4"), "DO5":self.IO.get("DO5"),
                                "AI1":self.IO.get("AI1"), "AI2":self.IO.get("AI2"), "AI3":self.IO.get("AI3"),
                                "AI4":self.IO.get("AI4"), "AI5":self.IO.get("AI5"),
                                "AO1":self.IO.get("AO1"), "AO2":self.IO.get("AO2"), "AO3":self.IO.get("AO3"),
                                "AO4":self.IO.get("AO4"), "AO5":self.IO.get("AO5")
                            }
                       }
                # mqtt.publish(self.pubTopic, json.dumps({"RUTIN":msg}))
                mqtt.client.publish(message_or_topic=self.pubTopic, payload=json.dumps({"RUTIN":msg}))
                self.MSGID +=1
            except:
                print("rutin send error")

            t1 = time.time()
            while (time.time() - t1 < self.rutinSend):
                time.sleep(1)
                if (self.stop_all_thread == True):
                    break
            if (self.stop_all_thread == True):
                print("stop RutinSend thread")
                break

    def RutinData(self, rutinData):
        print("rutinData ", rutinData)
        for io in rutinData:
            self.IO[str(io)] = rutinData.get(io)

    def DigitalData(self, digitalData):
        print("digitalData ", digitalData)
        for io in digitalData:
            if(dev.get(str(io), None) != None):
                dev[str(io)] = digitalData.get(io)

    def MessageParser(self, jsonData):
        # TODO: gelen verileri parse et ilgili cihazin loglarina kaydet
        try:
            if (jsonData.get("RUTIN")):
                data = jsonData.get("RUTIN")
                self.RutinData(data)
            elif (jsonData.get("DIGITAL")):
                data = jsonData.get("DIGITAL")
                self.DigitalData(data)
        except:
            print("MessageParser error")

    def start(self):
        x = threading.Thread(target=self.RutinRead, args=())
        x.start()
        x = threading.Thread(target=self.RutinSend, args=())
        x.start()




device = Device(uid="1234567892", slaveId=1)


@mqtt.on_connect()
def connect(client, flags, rc, properties):
    mqtt.client.subscribe(device.subTopic) #subscribing backend topic
    print("Connected: ", client, flags, rc, properties)

@mqtt.on_message()
async def message(client, topic, payload, qos, properties):
    print("Received message: ",topic, payload)




    try:

        jsonData = payload.decode('utf-8')
        jsonData = eval(jsonData)


        x = threading.Thread(target=device.MessageParser, args=(jsonData,))
        x.start()
    except:
        print("on_message error")
    return 0

@mqtt.on_disconnect()
def disconnect(client, packet, exc=None):
    print("Disconnected")

@mqtt.on_subscribe()
def subscribe(client, mid, qos, properties):
    print("subscribed", client, mid, qos, properties)

########################################## END-POINTS START #############################################
users = {
    "username":"bekir",
    "password":"123",
    "id":1
}

@app.get("/")
async def func(request:Request):
    b=device.IO #burda almıyo güncel demekki iolar güncellenmiyo

    b = {"motor cihaz durumu : ": b}
    return b

@app.get("/categories")
async def func(request:Request):
    data = [
        {"id":1, "name":"Device-1"},
        {"id":2, "name":"Device-2"},
    ]
    return data

@app.post("/login")
async def func(request:Request):
    body_data = await request.body()
    data = eval(body_data)
    print("st rent body data : ", data)

    return [users]

@app.post("/digital")
async def digital(request:Request):
    headerDatas = dict(request.headers.items())
    print("headerDatas ", headerDatas)
    body_data = await request.body()
    data = eval(body_data)
    print("st rent body data : ", data)
    x = threading.Thread(target=device.MessageParser, args=(data,))
    x.start()
    data = {"result": "ok"}
    return data


########################################### END-POINTS END ############################################

@app.on_event("startup")
async def startup_event():
    print("Start...")
    device.start()



@app.on_event("shutdown")
def shutdown_event():
    device.stop_all_thread = True
    print("End...")

