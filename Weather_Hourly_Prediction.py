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

#creating list
thana_list = df.THANA_NAME.values.tolist()
district_list = df.DISTRICT_NAME.values.tolist()
thana_code_list = df.THANA_CODE.values.tolist()
latlong_list = df.Latitude_Longitude.values.tolist()

#creating json array
json_thana = []
json_district = []
json_code = []
json_latlong = []

#get data from api 
for latlong,thana,district,code in zip(latlong_list, thana_list, district_list, thana_code_list):
    url = 'http://api.weatherapi.com/v1/forecast.json?key=key&q='+latlong+'&days=3&aqi=no&alerts=no'   
    r = http.request('GET', url)
    if (r.status==200):
        data = json.loads(r.data.decode('utf-8'))  
        json_district.append({'district':district})
        json_thana.append({'thana':thana})
        json_code.append({'thana_code':code})
        json_latlong.append(data)         
    else:
        print(thana+" weather api call unsucessfull")
        
#create data frame for weather api data
df_weather= json_normalize(json_latlong)
df_weather= pd.json_normalize(df_weather)

#drop hour tables
df_weather.drop(df_weather.iloc[:, 7:33], inplace=True, axis=1)

#creating date time columns
df_weather['event_date'] = pd.to_datetime(df_weather['forecast.forecastday.hour.time']).dt.date
df_weather['event_time']= pd.to_datetime(df_weather['forecast.forecastday.hour.time']).dt.time

#rounfing up event hour
df_weather['event_time'] = df_weather.event_time.astype(str).str[:2].astype(int)

#drop unnecessery columns
df_weather.drop(['location.region','location.country','location.localtime_epoch',
                 'forecast.forecastday.date_epoch','forecast.forecastday.hour.time','forecast.forecastday.day.maxtemp_f',
                'forecast.forecastday.day.mintemp_f','forecast.forecastday.day.avgtemp_f','forecast.forecastday.day.maxwind_mph',
                'forecast.forecastday.day.avgvis_km','forecast.forecastday.day.avgvis_miles','forecast.forecastday.day.avghumidity',
                'forecast.forecastday.day.condition.icon','forecast.forecastday.day.condition.code','forecast.forecastday.day.uv',
                'forecast.forecastday.astro.moon_illumination','forecast.forecastday.hour.time_epoch','forecast.forecastday.hour.temp_f',
                'forecast.forecastday.day.condition.text','forecast.forecastday.hour.condition.icon','forecast.forecastday.hour.condition.code',
                'forecast.forecastday.hour.wind_mph','forecast.forecastday.hour.pressure_mb','forecast.forecastday.hour.feelslike_f',
                'forecast.forecastday.hour.windchill_c','forecast.forecastday.hour.windchill_f','forecast.forecastday.hour.heatindex_c',
                'forecast.forecastday.hour.heatindex_f','forecast.forecastday.hour.dewpoint_c','forecast.forecastday.hour.dewpoint_f',
                'forecast.forecastday.hour.will_it_snow','forecast.forecastday.hour.chance_of_snow','forecast.forecastday.hour.vis_km',
                'forecast.forecastday.hour.vis_miles','forecast.forecastday.hour.gust_mph','forecast.forecastday.hour.gust_kph',
                'location.tz_id','forecast.forecastday.astro.moonrise','forecast.forecastday.astro.moonset',
                'forecast.forecastday.astro.moon_phase','forecast.forecastday.date','forecast.forecastday.astro.sunrise',
                'forecast.forecastday.astro.sunset','forecast.forecastday.hour.uv'], axis=1,inplace=True)

#drop hour tables
df_weather.drop(df_weather.iloc[:, 3:14], inplace=True, axis=1)

#humidity
df_weather= df_weather.assign(hourly_humidity_c = lambda x: round((x['forecast.forecastday.hour.humidity']-32)*(5/9)))
df_weather.drop(['forecast.forecastday.hour.humidity'], axis=1,inplace=True)

#rename columns
df_weather.rename(columns = {'location.name':'location'}, inplace = True)
df_weather.rename(columns = {'location.lat':'latitude'}, inplace = True)
df_weather.rename(columns = {'location.lon':'longitute'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.temp_c':'hourly_temperature_c'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.is_day':'hourly_is_day'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.condition.text':'hourly_condition'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.wind_kph':'hourly_wind_speed_kph'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.wind_degree':'hourly_wind_degree'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.wind_dir':'hourly_wind_direction'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.pressure_in':'hourly_preassure_in'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.precip_mm':'total_precipitation_mm'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.precip_in':'total_precipitation_in'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.cloud':'hourly_cloud'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.feelslike_c':'hourly_feelslike_c'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.will_it_rain':'hourly_will_it_rain'}, inplace = True)
df_weather.rename(columns = {'forecast.forecastday.hour.chance_of_rain':'hourly_chance_of_rain'}, inplace = True)

#create data frame for district
df_district = pd.json_normalize(json_district)
newdf_district = pd.DataFrame(np.repeat(df_district.values, 72, axis=0))
newdf_district.columns = df_district.columns

#create data frame for thana
df_thana = pd.json_normalize(json_thana)
newdf_thana = pd.DataFrame(np.repeat(df_thana.values, 72, axis=0))
newdf_thana.columns = df_thana.columns

#create data frame for thana code
df_code = pd.json_normalize(json_code)
newdf_code = pd.DataFrame(np.repeat(df_code.values, 72, axis=0))
newdf_code.columns = df_code.columns

#merger data for upzila name and weather api data 
df_weather.insert(0,'district_name',newdf_district)
df_weather.insert(1,'thana_name',newdf_thana)
df_weather.insert(2,'thana_code',newdf_code)

# convert the 'Date' column to datetime format
df_weather['event_date'] = pd.to_datetime(df_weather['event_date'])
df_weather['event_time'] = df_weather['event_time'].astype("string")

#open connection
cur = conn.cursor()

#delete previous data
DelsqlTxt = '''DELETE FROM BI_PROD.weather_hourly_prediction'''
cur.execute(DelsqlTxt)
conn.commit()

#insert data into oracle
dataInsertionTuples = [tuple(x) for x in df_weather.values]
sqlTxt = '''INSERT INTO BI_PROD.weather_hourly_prediction (district_name, thana_name, thana_code, location, latitude, longitute, hourly_temperature_c, 
       hourly_is_day, hourly_condition, hourly_wind_speed_kph, hourly_wind_degree, hourly_wind_direction, hourly_preassure_in,
       total_precipitation_mm, total_precipitation_in, hourly_cloud, hourly_feelslike_c, hourly_will_it_rain, hourly_chance_of_rain, event_date, event_time, hourly_humidity_c) 
       VALUES (:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22)'''

#open connection and execute
cur.executemany(sqlTxt, dataInsertionTuples)
conn.commit()

#close connection 
cur.close()
conn.close()
