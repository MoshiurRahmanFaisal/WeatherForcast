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
import cx_Oracle as oracle

#import date time
from datetime import date
from datetime import timedelta
yesterday = date.today() - timedelta(1)

#handle certificate verification and SSL warnings
http = urllib3.PoolManager(
    cert_reqs='CERT_REQUIRED',
    ca_certs=certifi.where())

#Adding Oracle Clinet 
oracle.init_oracle_client(lib_dir=r"C:\Users\moshiur.faisal\Oracle\instantclient_21_7")

#Creating SQL Connection
#add server credentials

#Reading Data from SQL
with conn.cursor() as cursor:
    sql = """select * from BI_PROD.weather_location_info """

#saing oracle data into data frame
df = pd.read_sql(sql, con=conn)

#Creating new columns
df["Latitude_Longitude"] = df["LATITUDE"].astype(str)+(",")+df["LONGITUDE"].astype(str)

#CReating lat_long list
district_list = df.DISTRICT_NAME.values.tolist()
thana_list = df.THANA_NAME.values.tolist()
thana_code_list = df.THANA_CODE.values.tolist()
latlong_list = df.Latitude_Longitude.values.tolist()

#lat_long array
json_district = []
json_thana= []
json_code = []
json_latlong = []

#get data from api 
for latlong,thana,district,code in zip(latlong_list, thana_list, district_list, thana_code_list):
    # print(i,j)
    url = 'http://api.weatherapi.com/v1/history.json?key=key&q='+latlong+'&dt='+str(yesterday)+'&end_dt='+str(yesterday)+'&hour=1'    
    r = http.request('GET', url)
    if (r.status==200):
        data = json.loads(r.data.decode('utf-8'))       
        json_thana.append({'thana':thana})
        json_district.append({'district':district})
        json_code.append({'thana_code':code})
        json_latlong.append(data)         
    else:
        print(thana+" weather api call unsucessfull")

#create data frame for weather api data
df_weather= json_normalize(json_latlong)
df_weather= pd.json_normalize(df_weather)

#drop unnecessary data
df_weather.drop(['location.region','location.country','location.localtime_epoch','location.localtime','location.tz_id',
                 'forecast.forecastday.date_epoch','forecast.forecastday.day.maxtemp_f','forecast.forecastday.day.mintemp_f',
                 'forecast.forecastday.hour.gust_kph','forecast.forecastday.day.condition.icon','forecast.forecastday.day.avgtemp_f',
                'forecast.forecastday.day.condition.code','forecast.forecastday.day.uv','forecast.forecastday.day.avgvis_km',
                'forecast.forecastday.day.maxwind_mph','forecast.forecastday.astro.moonrise','forecast.forecastday.astro.moonset',
                'forecast.forecastday.astro.moon_phase','forecast.forecastday.day.avgvis_miles'], axis=1,inplace=True)

#rename columns
df_weather.rename(columns = {'location.name':'location'}, inplace = True)
df_weather.rename(columns = {'location.lat':'latitude'}, inplace = True)
df_weather.rename(columns = {'location.lon':'longitute'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.date':'event_date'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.day.maxtemp_c':'maximum_temperature_c'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.day.mintemp_c':'minimum_temperature_c'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.day.avgtemp_c':'average_temperature_c'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.day.maxwind_kph':'max_wind_speed_kph'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.day.totalprecip_mm':'total_precipitation_mm'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.day.totalprecip_in':'total_precipitation_in'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.day.condition.text':'condition'}, inplace = True)

#drop hour tables
df_weather.drop(df_weather.iloc[:, 12:48], inplace=True, axis=1)

# convert the 'Date' column to datetime format
df_weather['event_date'] = pd.to_datetime(df_weather['event_date'])

#humidity
df_weather= df_weather.assign(average_humidity_c = lambda x: round((x['forecast.forecastday.day.avghumidity']-32)*(5/9)))
df_weather.drop(['forecast.forecastday.day.avghumidity'], axis=1,inplace=True)

#create data frame for district
df_district = pd.json_normalize(json_district)
newdf_district = pd.DataFrame(np.repeat(df_district.values, 7, axis=0))
newdf_district.columns = df_district.columns

#create data frame for thana
df_thana = pd.json_normalize(json_thana)
newdf_thana = pd.DataFrame(np.repeat(df_thana.values, 7, axis=0))
newdf_thana.columns = df_thana.columns

#create data frame for thana code
df_code = pd.json_normalize(json_code)
newdf_code = pd.DataFrame(np.repeat(df_code.values, 7, axis=0))
newdf_code.columns = df_code.columns

#merger data for upzila name and weather api data 
df_weather.insert(0,'district_name',newdf_district)
df_weather.insert(1,'thana_name',newdf_thana)
df_weather.insert(2,'thana_code',newdf_code)

#open connection
cur = conn.cursor()

#insert data to database
dataInsertionTuples = [tuple(x) for x in df_weather.values]
sqlTxt = '''INSERT INTO BI_PROD.weather_daily_history  ( district_name, thana_name, thana_code, location, latitude, longitute,event_date,
       maximum_temperature_c, minimum_temperature_c,
       average_temperature_c, max_wind_speed_kph, total_precipitation_mm,
       total_precipitation_in, condition, average_humidity_c) 
       VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15 )'''
       
#open connection and execute
cur.executemany(sqlTxt, dataInsertionTuples)
conn.commit()

#close connection 
cur.close()
conn.close()
