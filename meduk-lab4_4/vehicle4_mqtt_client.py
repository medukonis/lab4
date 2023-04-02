##################################################
# Name:         Michael Edukonis
# UIN:          677141300
# email:        meduk2@illinois.edu
# class:        CS437
# assignment:   Lab4
# date:         3/30/2023
##################################################
#vehicle4_mqtt_client.py - provides a listen only MQTT client
#Client should receive messages for itself only.  No other vehicle messages should be received.

# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import sys

#Path to your certificates, modify this
certificate_formatter = "/home/medukonis/Documents/University of Illinois/Spring 2023/Lab4/meduk-lab4_{}/meduk-lab4_{}.certificate.pem.crt"
key_formatter = "/home/medukonis/Documents/University of Illinois/Spring 2023/Lab4/meduk-lab4_{}/meduk-lab4_{}.private.pem.key"
device_id = "4"

class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = device_id
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
        print("vehicle"+device_id+" received payload {} from topic {}".format(self.device_id, message.payload, message.topic))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


print("Initializing Vehicle"+device_id+" MQTTClient...")

client = MQTTClient(device_id,certificate_formatter.format(device_id,device_id) ,key_formatter.format(device_id,device_id))
client.client.connect()
client.client.subscribeAsync("cars/veh"+device_id+"/report", 0, ackCallback=client.customSubackCallback)
time.sleep(2)

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        client.client.disconnect()
        print("Vehicle"+device_id+" client disconnecting...")
        sys.exit()


