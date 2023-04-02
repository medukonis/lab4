from datetime import datetime
dtime = datetime.now()
dtimestamp = dtime.timestamp()

milliseconds = round(dtimestamp * 1000)
print("Integer timestamp in milliseconds: ",
      milliseconds)
