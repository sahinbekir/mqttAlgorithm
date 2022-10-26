import requests
import urllib3
urllib3.disable_warnings()


#responsData = requests.post(url="https://localhost:3000/api/DeviceTelemetry", json=data, verify=False)





#responsData = requests.put(url="https://localhost:3000/api/IOScript/5", json=data, verify=False)

from dotenv import load_dotenv, dotenv_values
import time
ENV_PARAMETERS = dotenv_values()


class PostgreSqlOperation():




    # IMPORT: Son DT verileri alınır.
    def GetLastReadedState(self, ioname, deviceUid, slaveId):

        data = []
        try:
            responseData = requests.get(url="https://localhost:3000/api/DeviceTelemetry", verify=False)
            ioscripts = responseData.text
            #print("OKUMA",responseData)
            import json
            ioscripts = json.loads(ioscripts)
            #ioscriptsend = []
            #a=0
            #print(ioname,deviceUid,slaveId)
            #print("ioscrpits tekli:",len(ioscripts))
            #script=json.loads(ioscripts)
            #print(type(ioscripts),ioscripts[1])
            for script in ioscripts:
                if ( script['deviceUID']==deviceUid and script['ioName']==ioname and script['slaveId']==slaveId):

                    #print("OLDUU",script['deviceUID'],script['ioName'],script['slaveId'])
                    data.append(script)


            #print(data)
            #print(type(data))
            if (data != None):
                data = data[-1]


        except (Exception) as error:
            print("Tekli Okuma Error: \n",error)
        # finally:
        #     if self.dbConnection is not None:
        #         self.dbConnection.close()
        return data

    def UpdateIostate(self, data):  # {1:1}
        try:

            for id in data:
                query = {}
                if (type(data.get(id)) == dict):
                    for x in data.get(id):
                        query = str(data.get(id).get(x))
                        responsData = requests.put(url="https://localhost:3000/api/IOScript/" + str(x), json=query,
                                                   verify=False)
                else:
                    query = str(data.get(id))
                    responsData = requests.put(url="https://localhost:3000/api/IOScript/" + str(id), json=query,
                                               verify=False)

                #print("query",query,str(x))



        except (Exception) as error:
            print(error)
    # IMPORT: DB de connection değeri değişimi
    """def UpdateIostate(self, data): # {1:1}

        try:
            print("IO STATE GUNCELLE: \n", data)
            import json
            data=json.loads(data)

            print("DATA",data)
            for id in data:
                print("noluyo aq")
                if (type(data.get(id)) == dict):
                    for x in data.get(id):
                        query = str(data.get(id).get(x))

                        #print(query)
                        #aaa = json.loads(query)
                        responsData = requests.put(url="https://localhost:3000/api/IOScript/1", json={
    "ioScriptId": 1,
    "masterDeviceUID": "1234567891",
    "slaveDeviceUID": "1234567892",
    "masterSlaveId": 1,
    "slaveSlaveId": 1,
    "masterIOName": "DI1",
    "slaveIOName": "DO1",
    "lastValue": 1,
    "description": "Ş-D Otonom"
  }, verify=False)
                        #print(responsData)
                        #print("query")
                else:
                    query = str(data.get(id))

                    #aaa = json.loads(query)
                    responsData = requests.put(url="https://localhost:3000/api/IOScript/1", json={
    "ioScriptId": 1,
    "masterDeviceUID": "1234567891",
    "slaveDeviceUID": "1234567892",
    "masterSlaveId": 1,
    "slaveSlaveId": 1,
    "masterIOName": "DI1",
    "slaveIOName": "DO1",
    "lastValue": 1,
    "description": "Ş-D Otonom"
  },
                                               verify=False)
                    #print("guery","aaa")
                    #print(responsData)

            #print(aaa[1])
            #print(responsData,"HATAA")
        except (Exception) as error:


            print("Update IO State: \n",error)

        # finally:
        #     if self.dbConnection is not None:
        #         self.dbConnection.close()
"""
    # IMPORT: Connectiondan masteruid alınıp DT de son değer bulunur
    def GetAllRequestDatas(self):
        try:
            responseData = requests.get(url="https://localhost:3000/api/IOScript", verify=False)
            ioscripts = responseData.text
            #print("OKUUUUU",responseData)
            import json
            ioscripts = json.loads(ioscripts)
            # tum slaveIo listesi olmali
            #  masteruid ye göre slaveudlsi ayni olan tum degerler, ve en son slaveid
            # degisim olanlara gonderilecek deger
            degisenlerVal = {}
            rutinAllVal = {}
            dbUpdateIoScript = {}

            counter = 0
            counter1 = 0
            mUid = []
            sUid = []
            #print("ioscripts",ioscripts)
            #print("ioscriptslen", len(ioscripts))

            for script in ioscripts:
            #for i in range(len(ioscripts)):

                #script=ioscripts
                #print("script: ",script["ioScriptId"])
                # TODO: ilk olarak devicetelemetry tablosundan elimizdeki masterDeviceUid sine ait ilgili
                id = script["ioScriptId"]
                masterDeviceUid = script["masterDeviceUID"]
                slaveDeviceUid = script["slaveDeviceUID"]
                masterSlaveId = script["masterSlaveId"]
                slaveSlaveId = script["slaveSlaveId"]
                masterIoName = script["masterIOName"]
                slaveIoName = script["slaveIOName"]
                lastValue = script["lastValue"]  # eger okuancak deger ile bu farklı ise veriyi yolla.
                """id = 1
                masterDeviceUid = "1234567891"
                slaveDeviceUid = "1234567892"
                masterSlaveId = 1
                slaveSlaveId = 1
                masterIoName = "DI1"
                slaveIoName = "DO1"
                lastValue = 0"""
                mUid.append(masterDeviceUid)
                sUid.append(slaveDeviceUid)
                #print("GONDERILIYO:",masterIoName,masterDeviceUid,masterSlaveId)
                value = self.GetLastReadedState(masterIoName, masterDeviceUid, masterSlaveId)
                if (value == None): value = 0
                if(lastValue != value):
                    # degisenlerVal[counter] = {"id":id, "masterDeviceUid":masterDeviceUid, "slaveDeviceUid":slaveDeviceUid,
                    #                           "masterSlaveId":masterSlaveId, "slaveSlaveId":slaveSlaveId,
                    #                           "masterIoName":masterIoName, "slaveIoName":slaveIoName, "currentValue":value
                    #                           }
                    degisenlerVal[counter] = {"id":id, "slaveDeviceUid":slaveDeviceUid, "slaveSlaveId":slaveSlaveId,
                                                  "name": slaveIoName, "value":value
                                              }
                    dbUpdateIoScript[counter] = {id:value}
                    counter +=1

                # rutinAllVal[counter1] = {"id":id, "masterDeviceUid":masterDeviceUid, "slaveDeviceUid":slaveDeviceUid,
                #                           "masterSlaveId":masterSlaveId, "slaveSlaveId":slaveSlaveId,
                #                           "masterIoName":masterIoName, "slaveIoName":slaveIoName, "currentValue":value
                #                           }
                rutinAllVal[counter1] = {"id":id, "slaveDeviceUid":slaveDeviceUid, "slaveSlaveId":slaveSlaveId,
                                           "name": slaveIoName, "value":value
                                          }
                counter1 += 1

            mUid = list(set(mUid))
            sUid = list(set(sUid))

            rutinValues = {}
            checkValues = {}
            for index in sUid:
                tempValues = []
                for item in degisenlerVal:
                    temp = degisenlerVal.get(item)
                    if (temp.get("slaveDeviceUid") == index):
                        tempValues.append((temp.get("slaveSlaveId"), temp.get("name"), temp.get("value")))
                if (len(tempValues) > 0):
                    checkValues[index] = tempValues

            for index in sUid:
                tempValues = []
                for item in rutinAllVal:
                    temp = rutinAllVal.get(item)
                    if (temp.get("slaveDeviceUid") == index):
                        tempValues.append((temp.get("slaveSlaveId"), temp.get("name"), temp.get("value")))
                rutinValues[index] = tempValues

            datas = rutinValues
            rutinDatas = []
            for index in datas:
                endData = {}
                val = datas.get(index)
                endData["UID"] = index
                for param in val:
                    slaveId = param[0]
                    ioName = param[1]
                    ioValue = param[2]

                    if (endData.get(slaveId) == None):
                        # ilk ekle
                        endData[slaveId] = {ioName: ioValue}
                        continue
                    endData[slaveId][ioName] = ioValue

                uid = endData.get("UID")
                del endData["UID"]
                for sendD in endData:
                    valX = endData.get(sendD)
                    e = {"UID": uid, "SLAVEID": sendD}
                    for x in valX:
                        e[x] = valX.get(x)
                    rutinDatas.append(e)



            datas = checkValues
            checkedDatas = []
            for index in datas:
                endData = {}
                val = datas.get(index)
                endData["UID"] = index
                for param in val:
                    slaveId = param[0]
                    ioName = param[1]
                    ioValue = param[2]

                    if (endData.get(slaveId) == None):
                        # ilk ekle
                        endData[slaveId] = {ioName: ioValue}
                        continue
                    endData[slaveId][ioName] = ioValue

                uid = endData.get("UID")
                del endData["UID"]
                for sendD in endData:
                    valX = endData.get(sendD)
                    e = {"UID": uid, "SLAVEID": sendD}
                    for x in valX:
                        e[x] = valX.get(x)
                    checkedDatas.append(e)


        except (Exception) as error:

            print(error)
            print("GetAllRequestDatas error")

        return rutinDatas, checkedDatas, dbUpdateIoScript






