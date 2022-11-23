#import libraries
import urllib3
from urllib3 import request
import certifi
import json
from json_normalize import json_normalize
import pandas as pd
import numpy as np
import schedule 
import datetime

# def call_me(): 
#     #handle certificate verification and SSL warnings
http = urllib3.PoolManager(
    cert_reqs='CERT_REQUIRED',
    ca_certs=certifi.where())

#get lat long data from csv
loc=pd.read_csv("Upazila_Lat-Long.csv")
# print(loc.head(5))
loc_list = loc.lat_long.values.tolist()
up_list = loc.upazila_name.values.tolist()
# print(up_list[0])

json_arr1 = []
json_arr2 = []

#get data from api 
for i, j in zip(loc_list, up_list):
    # print(i,j)
    url = 'http://api.weatherapi.com/v1/forecast.json?key=df01195e56c846298e145214220911&q='+i+'&days=7&aqi=no&alerts=no'
    r = http.request('GET', url)
    if (r.status==200):
        data = json.loads(r.data.decode('utf-8'))       
        json_arr2.append({'upazila':j})
        json_arr1.append(data)   
        # print(data)        
    else:
        print(j+" weather api call unsucessfull")

#create data frame for upazila
df_upazila = pd.json_normalize(json_arr2)
newdf_upazila = pd.DataFrame(np.repeat(df_upazila.values, 168, axis=0))
newdf_upazila.columns = df_upazila.columns

#create data frame for weather api data
df_weather= json_normalize(json_arr1)
df_weather= pd.json_normalize(df_weather)
# print(df_weather)

#merger data for upzila name and weather api data 
df_weather.insert(0,'Upazila',newdf_upazila)
# print(df2.head(1))

#save data into csv
x = str(datetime.date.today())
df_weather.to_csv("weather_data_upazila_future_"+x+".csv")

# schedule.every().day.do(call_me)
# while True: 
#   schedule.run_pending()