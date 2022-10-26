import threading
import time
from urllib import response

from fastapi import FastAPI, Request
from fastapi_mqtt import FastMQTT, MQTTConfig
from starlette.middleware.cors import CORSMiddleware

from db.pgdb import PostgreSqlOperation
from telemetryOperations import Functions
from starlette.middleware.cors import CORSMiddleware
curuserid=1
app = FastAPI()

origins = [
    "https://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mqtt_config = MQTTConfig(host = "broker.hivemq.com",
    port= 1883,
    keepalive = 60,
    username="",
    password="",
    version=4)

# Delcom broker
# mqtt_config = MQTTConfig(host = "23.88.63.32",
#     port= 1883,
#     keepalive = 60,
#     username="delcomrf",
#     password="delcomrf",
#     version=4)

mqtt = FastMQTT(
    config=mqtt_config)


mqtt.init_app(app)

#
mqtt_sub_topics = "LynxDevices"

dbOperations = PostgreSqlOperation()

functions = Functions(mqtt=mqtt)
@mqtt.on_connect()
def connect(client, flags, rc, properties):
    global functions
    mqtt.client.subscribe(mqtt_sub_topics) #subscribing mqtt topic
    print("Connected: ", client, flags, rc, properties)
    functions.mqtt = mqtt
    functions.ok = True

@mqtt.on_message()
async def message(client, topic, payload, qos, properties):
    print("Received message: ",topic, payload)
    jsonData = payload.decode('utf-8')
    jsonData = eval(jsonData)

    x = threading.Thread(target=functions.ReceiveDataParser, args=(jsonData,))
    x.start()

    return 0

@mqtt.on_disconnect()
def disconnect(client, packet, exc=None):
    print("Disconnected")

@mqtt.on_subscribe()
def subscribe(client, mid, qos, properties):
    print("subscribed", client, mid, qos, properties)


@app.get("/")
async def func():

    data = {"FASTAPI BACKEND"}
    return data
from starlette.middleware.cors import CORSMiddleware
origins = [
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/ioset")
async def ioset(request:Request):
    print("GIRIYO")
    body_data = await request.body()
    import json
    body_data = json.loads(body_data)
    #print(body_data)

    uid=body_data['UID']
    data = {
        "DIGITAL":

            {
                "UID": body_data['UID'],
                "SLAVEID": body_data['SLAVEID'],
                 body_data['DNAME']:int(body_data['DID'])
            }

    }
    print("Emir:",data)
    topic = "devices/" + uid + "/messages"
    mqtt.client.publish(message_or_topic=topic, payload=data)
    #time.sleep(20)




    return 0




@app.on_event("startup")
async def startup_event():
    global mqtt
    print("Start...")
    functions.start(mqtt)





@app.on_event("shutdown")
def shutdown_event():
    print("End...")
    functions.endThread = True
    time.sleep(5)

# if __name__ == "__main__":
#     uvicorn.run(app, host="localhost", port=3000)