##################################################
# Name:         Michael Edukonis
# UIN:          677141300
# email:        meduk2@illinois.edu
# class:        CS437
# assignment:   Lab4
# date:         3/30/2023
##################################################
#lambda.py

import json
import boto3
from datetime import datetime


###########################################################################
# Function: lambda_handler()
# Inputs:   dict, LambdaContext
# Outputs:  ?? depends
# notes:    function lives and runs on aws.  For
#           lab4, the function will fire upon
#           receipt of a MQTT message to
#           cars/+/report + is a wildcard.
#           Function will
#               *Take the vehicle data received (the event)
#               *Parse it
#               *Calculate highest CO2 reading for each vehicle
#               *Post data to database for later analysis
#               *Send an MQTT message to each vehicle's own topic
#                   -message will have the specific vehicle's high
#                    CO2 reading.
###########################################################################
def lambda_handler(event, context):
    #TODO1: Get your data
    #event payload provides the necessary data.  The client is programmed to only provide what is necessary
    #based on lab instructions.  This is, from each row, timestamp, CO2 reading, and vehicle id.
    #TODO cut the vehicle id out if we can somehow get that piece of information from the message header? Saves bandwidth

    #The event is the incoming MQTT message
    readings = event

    #db object is to connect to aws dynamodb already setup
    db = boto3.client('dynamodb')

    #dynamodb does not allow an auto increment id so we had to get creative to supply a primary key that wont duplicate
    dtime = datetime.now()
    dtimestamp = dtime.timestamp()
    time_ms = round(dtimestamp * 1000)

    #TODO2: Calculate max CO2 emission (dictionary comprehension gets the row with the max CO2 reading)
    maxCO2row = max(readings, key=lambda item:item['vehicle_CO2'])

    #parse resulting dictionary of max CO2 value with accompanying data into variables.  Will put these into sql database
    maxCO2reading = maxCO2row['vehicle_CO2']
    vehicle = maxCO2row['vehicle_id']
    time = maxCO2row['timestep_time']

    #post to dynamodb
    db.put_item(TableName='emissions', Item={'id':{'N': str(time_ms)},'time':{'N':str(time)}, 'co2':{'N':str(maxCO2reading)}, 'vehicle':{'S':vehicle}, 'readings_dict':{'S': str(maxCO2row)}})

    #TODO3: Return the result
    client = boto3.client('iot-data', region_name='us-east-1')
    # Change topic, qos and payload
    response = client.publish(topic="cars/"+vehicle+"/report",payload=str(maxCO2row))

    return response
