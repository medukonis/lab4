import json

# Opening JSON file
f = open('subscription(11).json')

# returns JSON object as
# a dictionary
data = json.load(f)
readings = data["messages"][0]["payload"][0]
print(readings)

''''  #jsonString = json.dumps(readings)
    #toJson = readings.to_json('file.json', orient = 'records') #very weird issue in testing this.  Converting to JSON list variable was somehow not in proper format for dictionary parse below.  Save to json file and then reload and it is fine.  Inefficient but ran out of time to debug. Lambda provides 500MB space but data is not persistent across executions.
    #TODO: Greengrass lambda will run on core machine. Not sure if persistent? Doesn't really matter.

    #with open("file.json") as file:
    #    data_list = json.load(file)


    #TODO2: Calculate max CO2 emission (dictionary comprehension gets the row with the max CO2 reading)
    maxCO2row = {k: max(d[k] for d in readings) for k in readings[0].keys()}

    #parse resulting dictionary of max CO2 value with accompanying data into variables.  Will put these into sql database
    maxCO2reading = max_dict['vehicle_CO2']
    vehicle = max_dict['vehicle_id']
    time = max_dict['timestep_time']

    #TODO3: Return the result
    #Each vehicle should only be subscribed to its own topic
    client.publish(topic="cars/" + vehicle + "/report", payload=json.dumps(maxCO2row))

    return maxCO2row'''
