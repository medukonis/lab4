##################################################
# Name:         Michael Edukonis
# UIN:          677141300
# email:        meduk2@illinois.edu
# class:        CS437
# assignment:   Lab4
# date:         3/30/2023
##################################################
#retrieve_data.py - this script will pull vehicle data from dynamodb database
#pandas and matplotlib will create the data bar chart

#Reference only.  Lab instructions state to enable and utilize aws sagemaker and pipeline data.
#See sagemaker_jupyter.py for that code.

#will use jupyter notebook to visualize.
#NOTE:Dynamodb stores everything as string data type but also stores "metadata"
#of what the element should be.  This allows the user to perform actions on
#the data like math on stored numbers even though they are stored
#as a string.  Unfortunately, all of that metadata comes with the data export.
#util will clean it up into standard json format so we can use it.

from IPython.display import HTML
import boto3
import json
import pandas as pd
from dynamodb_json import json_util as util
import matplotlib.pyplot as plt

db              = boto3.resource('dynamodb', region_name="us-east-1")
table           = db.Table("emissions")
response        = table.scan()
data            = response['Items']
regular_json    = util.loads(data)
df              = pd.DataFrame(regular_json)
ax              = df.plot.bar(x="vehicle", y="co2", color=(0.2, 0.4, 0.6, 0.6), edgecolor='green')

#add the CO2 actual reading numbers to top of bars
ax.bar_label(ax.containers[0])

#plot the chart
plt.show()

#in jupyter notebook use commands
#%load retrieve_data.py to view python script
#%run retrieve_data.py to run python script

#SELECT * FROM lab_4_data_store LIMIT 10
from_unixtime(timestamp_field)
