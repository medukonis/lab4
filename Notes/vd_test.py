import pandas as pd
import json
import csv
#Path to the dataset, modify this
fields = ['timestep_time', 'vehicle_CO2', 'vehicle_id']
data_path = "vehicle{}.csv"
print("Loading vehicle data...")


'''for i in range(5):
    df = pd.read_csv(data_path.format(i), usecols=fields)   #only interested in timestamp and CO2 level
    df2=df.loc[df['vehicle_CO2'].idxmax()]                  #find the max CO2 value
    dataFrame = df2.to_json(orient = 'columns')             #pandas function to convert csv to json
    #print(df2)                                             #DEBUG prints timestamp and max CO2 value
    data.append(dataFrame)   '''

for i in range(5):
    test_list = []
    df = pd.read_csv(data_path.format(i), usecols=fields)   #only interested in timestamp and CO2 level
    #Change to JSON - kept getting a attribute error when converting to json within the program.  workaround is write to temp file and import as json
    #Fortunately AWS Lambda gives 500mb of temp disk space that is reliable for only each execution.  That is all we need.
    dataFrame = df.to_json('file.json', orient = 'records')

    with open("file.json") as file:
        test_list = json.load(file)

    # Using dictionary comprehension
    max_dict = {k: max(d[k] for d in test_list) for k in test_list[0].keys()}

    maxCO2 = max_dict['vehicle_CO2']
    vehicle = max_dict['vehicle_id']
    time = max_dict['timestep_time']
    print("maxCO2: %s" % maxCO2)
    #print("All keys maximum: ", max_dict)




