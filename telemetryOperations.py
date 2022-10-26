import threading
import time
import json
import ssl
from db.pgdb import PostgreSqlOperation



import requests
import urllib3
urllib3.disable_warnings()


#

class Functions():
    def __init__(self, mqtt):
        self.db = PostgreSqlOperation()
        #self.db.connection()
        self.mqtt= mqtt
        self.endThread = False
        self.ok = False

    def RutinData(self, data):
        """
        gelen her IO bilgisi sisteme tanimli olsada olmasada kaydediliyor.
        :param data:
                {'UID': '1234567891', 'MSGID':1, "timestamp": '01/01/2001 12:12:12', 'SLAVEID': 1,
                    "IO":{'DI1': 0, 'DI2': 0, 'DI3': 0, 'DO1': 0, 'DO2': 0, 'DO3': 0}
                    }
        :return:
        datetime.datetime.now()
        deviceUid ye ait register tablosundan kayitli io isimlerini çek liste olarak

        deviceUid	slaveId 	messageId	ioName	value time
        UID         SLAVEID     MSGID   DI1      0

        """

        io = data.get("IO", None)



        for key in io:
            values = {"deviceTelemetryId":0,"deviceUID":data.get("UID"), "slaveId":data.get("SLAVEID"),
                      "messageId":data.get("MSGID"), "iOName":key, "value":io.get(key), "date":data.get("timestamp")}
            deviceUID = data.get("UID")
            ioNamee = key
            messageId = data.get("MSGID")
            slaveId = data.get("SLAVEID")
            lastvalue = io.get(key)
            date = data.get("timestamp")
            """
            print(deviceUID,type(deviceUID))
            
            print(slaveId, type(slaveId))
            
            print(messageId, type(messageId))
            
            print(ioNamee, type(ioNamee))
            
            print(lastvalue, type(lastvalue))
            
            print(date, type(date))"""
            value956s={
  "deviceTelemetryId": "0",
  "deviceUID": "deviceUID",
  "slaveId":"1",
  "messageId": "1",
  "ioName": "ioNamee",
  "value": "1",
  "date": "11-11-2022"
}



            #print(type(lastvalue))
            headers = {'content-type': 'application/json'}
            myheaders = {'Content-type': 'application/form', 'Accept': 'application/form'}
            session = requests.Session()
            #from urllib.parse import urlencode
            #postdata = urlencode(value956s)
            #print("kalsd",len(value956s))
            aaa={
  "deviceTelemetryId": 0,
  "deviceUID": "1234567891",
  "slaveId": 1,
  "messageId": 1,
  "ioName": "DI1",
  "value": 1,
  "date": "2022-10-10T13:59:46.907Z"
}
            responsData = requests.post(url="https://localhost:3000/api/DeviceTelemetry", json=
            aaa

                                        , verify=False)
            #print("RES POST1 DT",responsData)

    def AlertData(self, data):
        """
        TODO : Kayitli kisiye email gonder
        :param data:
                {'UID': '1234567891', 'MSGID':1, "timestamp": '01/01/2001 12:12:12', 'SLAVEID': 1,
                    "AL": [
                                {'NAME':'DI1', 'VALUE': 0, 'MSG':"depo samandira girisi druum degisti' },
                                {'NAME':'DI1', 'VALUE': 0, 'MSG':"depo samandira girisi druum degisti' },
                            ]
                }
        :return:
        """
        al = data.get("AL", None)
        message=""
        deviceTelemetryId=0
        for item in al:
            value = {"deviceUID": data.get("UID"), "slaveId": data.get("SLAVEID"), "messageId": data.get("MSGID"),
                      "ioName": item.get("NAME"), "value": item.get("VALUE"), "message":item.get("MSG"),
                     "date":data.get("timestamp")}
            message=item.get("MSG")
            deviceTelemetryId=item.get("NAME")
            responsData = requests.post(url="https://localhost:3000/api/DeviceAlertTelemetry", json={"alertId":0,"message":"ADSD","deviceTelemetryId":261}, verify=False)
            #print("RES POST2 DT", responsData)
    def ReceiveDataParser(self, jsonData):
        """

        :param payload:
        :return:

       {'RUTIN':{ 'UID': '1234567891', 'MSGID':1, "timestamp": '01/01/2001 12:12:12', 'SLAVEID': 1, 'DI1': 0, 'DI2': 0, 'DI3': 0, 'DO1': 0, 'DO2': 0,
       'DO3': 0}}

       {'ALERT':{'UID': '1234567891', 'MSGID':1, "timestamp": '01/01/2001 12:12:12', 'SLAVEID': 1, 'DI1': 0, 'DI1MSG':"depo samandira girisi druum degisti' }}




        """
        if (jsonData.get("RUTIN")):
            data = jsonData.get("RUTIN")
            self.RutinData(data)
        elif (jsonData.get("ALERT")):
            data = jsonData.get("ALERT")
            self.AlertData(data)

    def CheckState(self):
        """
        TODO: ioscript tablosundaki listeye gore devicetelemetry dosyasini kontrol et egisimvarsa dlave e ilet.
        :return:
        """
        while(1):
            try:
                rutinAllVal, checkValues, dbUpdateIoScript = self.db.GetAllRequestDatas()

                for i in checkValues:
                    pubTopic = "devices/" + str(i.get("UID")) + "/messages"

                    #print(pubTopic, i)

                    if (self.ok == True):
                        self.mqtt.client.publish(message_or_topic=pubTopic, payload=json.dumps({"DIGITAL": i}))
                    time.sleep(2)


                if (len(dbUpdateIoScript) > 0 and self.ok == True):
                    #print("girdi bastı1", dbUpdateIoScript)
                    self.db.UpdateIostate(dbUpdateIoScript)

            except:
                print("CheckState error")
            time.sleep(5)
            if(self.endThread == True):
                break

    def RutinSendData(self):
        """
        TODO: proje listesini oku
                    her projeye ait master datalarına göre  slave e veri yolla
                    cihazlara ait guncel data DeviceTelemetry tablosundan okunacak
        :return:
        """
        while(1):
            try:

                rutinAllVal, checkValues, dbUpdateIoScript = self.db.GetAllRequestDatas()


                for i in rutinAllVal:

                    pubTopic = "devices/" + str(i.get("UID")) + "/messages"

                    if(self.ok == True):
                        self.mqtt.client.publish(message_or_topic=pubTopic, payload=json.dumps({"DIGITAL": i}))
                    time.sleep(2)


                if (len(dbUpdateIoScript) > 0 and self.ok == True):
                    #print("girdi bastı2", dbUpdateIoScript)
                    self.db.UpdateIostate(dbUpdateIoScript)

            except:
                print("CheckState error")
            time.sleep(60)
            if (self.endThread == True):
                break

    def start(self, mqtt):
        self.mqtt = mqtt
        x = threading.Thread(target=self.CheckState, args=())
        x.start()
        x = threading.Thread(target=self.RutinSendData, args=())
        x.start()
        pass

#
# f = Functions("")
# f.CheckState()
