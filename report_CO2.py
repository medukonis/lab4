##################################################
# Name:         Michael Edukonis
# UIN:          677141300
# email:        meduk2@illinois.edu
# class:        CS437
# assignment:   Lab4
# date:         3/30/2023
##################################################
#report_CO2.py - this is the renamed device emulator for all 5 "vehicles"
#this script will read in the 5 CSV data files, process the data and send
#the data by MQTT message to the pertinent subsciptions.  It will rotate
#among the data and subscriptions for the five vehicles.

# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np

#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 5

#Path to the dataset and fields we are interested in for lab4
data_path = "vehicle{}.csv"
#The CSV file are large with many attributes and lines.  This will cut down on the amount of transmitted data.
#the program will only send data from these 3 attributes or columns.
fields = ['timestep_time', 'vehicle_CO2', 'vehicle_id']

#Path to your certificates, modify this
certificate_formatter = "/home/medukonis/Documents/University of Illinois/Spring 2023/Lab4/meduk-lab4_{}/meduk-lab4_{}.certificate.pem.crt"
key_formatter = "/home/medukonis/Documents/University of Illinois/Spring 2023/Lab4/meduk-lab4_{}/meduk-lab4_{}.private.pem.key"


class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("a1t3toje5okjxz-ats.iot.us-east-1.amazonaws.com", 8883)
        self.client.configureCredentials("/home/medukonis/Documents/University of Illinois/Spring 2023/Lab4/AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage


    def customOnMessage(self,message):
        #TODO3: fill in the function to show your received message
        print("client {} received payload {} from topic {}".format(self.device_id, message.payload, message.topic))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self, topic, Payload):                           #added topic variable
        #TODO4: fill in this function for your publish
        topic_string = "cars/veh"+str(topic)+"/report"       #will rotate the topic string based on what vehicle is being processed.
        self.client.subscribeAsync(topic_string, 0, ackCallback=self.customSubackCallback)
        self.client.publishAsync(topic_string, Payload, 0, ackCallback=self.customPubackCallback)



print("Loading vehicle data...")
data = []
for i in range(5):
    df = pd.read_csv(data_path.format(i), usecols=fields)   #only interested in 'timestep_time', 'vehicle_CO2', and 'vehicle_id'.  Fields variable set above.
    #df2=df.loc[df['vehicle_CO2'].idxmax()]                 #pandas function - find the max CO2 value if we wanted to do this on the vehicle instead of on the cloud
    dataFrame = df.to_json(orient = 'records')              #pandas function - convert csv to json and orient the output appropriately
    #print(df2)                                             #DEBUG prints timestamp and max CO2 value
    data.append(dataFrame)
#    with open("test.json", "w") as outfile:
#        outfile.write(dataFrame)


print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    client = MQTTClient(device_id,certificate_formatter.format(device_id,device_id) ,key_formatter.format(device_id,device_id))
    client.client.connect()
    clients.append(client)

while True:
    print("send now?")
    x = input()
    if x == "s":
        for i,c in enumerate(clients):
            #print(data[i])                               #debug code to get a look at what is being sent
            c.publish(i, data[i])
            #c.publish("hello from device {}".format(i))  #debug code for testing other clients for receipt.

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed")

    time.sleep(3)

