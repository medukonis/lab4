##################################################
# Name:         Michael Edukonis
# UIN:          677141300
# email:        meduk2@illinois.edu
# class:        CS437
# assignment:   Lab4
# date:         4/1/2023
##################################################
#This code utilized in AWS sagemaker jupyter notebook to demonstrate data pipeline

import boto3
from IPython.display import HTML
import json
import pandas as pd
import matplotlib.pyplot as plt

# create IoT Analytics client
client = boto3.client('iotanalytics')

#Now we can get the data location (URL) for the given dataset and start working with the data (In order to need to perform get_dataset_content, you need to grant iot analytics corresponding IAM permission):

dataset = "lab4_dataset"
dataset_url = client.get_dataset_content(datasetName = dataset)['entries'][0]['dataURI']

# start working with the data
#print(dataset_url)
df = pd.read_csv(dataset_url, header=0)
#df        #print all lines
#df.tail() #pandas function gives last five entries
#df.plot.bar(x="vehicle_id", y="vehicle_co2", color=(0.2, 0.4, 0.6, 0.6), edgecolor='green')
df.tail().plot.bar(x="vehicle_id", y="vehicle_co2", color=(0.2, 0.4, 0.6, 0.6), edgecolor='green')
