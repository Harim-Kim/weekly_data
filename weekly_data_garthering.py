#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime
from time import gmtime, strftime
import mariadb
from sqlalchemy import create_engine
import pymysql


# In[2]:


def date_to_strdate(date):
    return     date.strftime('%Y-%m-%d %H:%M:%S')


# In[3]:


pan_conn = pymysql.connect(host='ec1-it-prd-wibledrive-rds.clv6o5ibky6t.eu-central-1.rds.amazonaws.com', user='devops', password='Maas1223@' , charset='utf8')


# In[4]:


core_conn = pymysql.connect(host='ec1-it-prd-core-rds.clv6o5ibky6t.eu-central-1.rds.amazonaws.com', user='devops', password='Maas1223@' , charset='utf8')


# In[5]:


# 월요일 00시에 돌아간다는 가정. 아무튼 월요일에 돌아야함.
today = datetime.date.today() 
last_monday = today - datetime.timedelta(days=today.weekday()+7)
last_sunday = today - datetime.timedelta(days=today.weekday()+1)


# In[6]:


start_date = last_monday.strftime("%Y-%m-%d %H:%M:%S")
print("data starting on:",start_date)


# In[7]:


end_date = last_sunday.strftime("%Y-%m-%d")
end_date = end_date[:11]+' 23:59:59'
print("data end on:",end_date)


# # table 미리 불러오기 with pandas

# In[8]:


print("get data from DB")
#전체 예약 정보
query = 'SELECT * FROM maas_db_reservation.reservations'
print(query)
reservations = pd.read_sql_query(query,core_conn)


# In[9]:


query = 'SELECT * FROM maasfms.reservation_short'
print(query)
reservation_short = pd.read_sql_query(query,pan_conn)


# In[10]:


#mp 테이블
query = "select p.reservation_id, (case when p.pg_tid is null then 'Offline' else 'Online' end) on_off from maasfms.payment p left join maasfms.payment_history ph on p.reservation_id = ph.reservation_id left join maasfms.reservation_short rs on p.reservation_id = rs.reservation_id where p.deleted_at is null and p.reservation_count between 0 and 1"
print(query)

mp = pd.read_sql_query(query,pan_conn)


# In[11]:


#전체 rental vehicle 테이블
query = 'SELECT * FROM maas_db_reservation.rental_vehicles where deleted_at is null'
print(query)

rental_vehicles = pd.read_sql_query(query,core_conn)


# In[12]:


query = "select * from maasfms.location where name not in ('Songdong-Gu', 'Seoul','KMIT','Test','이종원','KME')"
print(query)

location = pd.read_sql_query(query,pan_conn)
print("check location names in the system:",location.name.unique())
print()


# In[13]:


# id컬럼명 수정
location["pickup_station_id"] = location["ID"]


# In[14]:


#전체 vehicle
query = 'SELECT * FROM maas_db_vehicle.vehicles'
print(query)
vehicles = pd.read_sql_query(query,core_conn)
vehicles["business_id"] = vehicles["group_id"]


# In[15]:


#전체 vehicle model
query = 'SELECT * FROM `maas_db_vehicle-model`.vehicle_models '
print(query)
vehicle_models = pd.read_sql_query(query,core_conn)


# In[16]:


vehicle_models1 = vehicle_models[vehicle_models["is_standard"]==0]


# In[17]:


# 스탠다드 모델만 가져옴
print("get standard vehicle model")
vehicle_models2 = vehicle_models[vehicle_models["is_standard"]==1]
# vehicle_models2 = vehicle_models2[vehicle_models2["deleted_at"].isna()]


# In[18]:


query = "select * from maasfms.chargevehicle where deleted_at is null "
print(query)
chargevehicle = pd.read_sql_query(query, pan_conn)


# In[19]:


query = "select * from maasfms.business where deleted_at is null"
print(query)
business = pd.read_sql_query(query, pan_conn)
business["business_id"] = business["id"]


# In[20]:


query = "select cv.vehicle_id           from maasfms.chargevehicle cv           join maasfms.charge c on cv.charge_id = c.id          where 1 = 1            and cv.deleted_at is null            and c.deleted_at is null"
print(query)
vehicle_charge = pd.read_sql_query(query, pan_conn)


# In[21]:


query = "select reservation_id , min(created_at) as reservated_at from maas_db_reservation.reservations where created_at >= '2020-09-20 22:00:00'            and created_at <= '"+ end_date+ "'       group by reservation_id"
print(query)
reservated_at = pd.read_sql_query(query, core_conn)


# In[22]:


query = "select * from maasfms.country "
print(query)
country = pd.read_sql_query(query, pan_conn)


# In[23]:


vehicle_models1 = vehicle_models1.sort_values(by="created_at", ascending=True)
vehicle_models1 = vehicle_models1.drop_duplicates(subset=["vehicle_model_id","standard_model_id","vehicle_model_name"], keep="last")


# In[ ]:





# In[25]:


vehicles1 = vehicles.drop_duplicates(subset=["vehicle_id","model_id"], keep="last")


# In[26]:



print("============================ query end ==============================")
print("closing connections")
# pan_conn.close()
# core_conn.close()
print("============================ stat start ==============================")
print("예약 일수 별 예약 통계")
#reservations join rental vehicle
print("join reservations, rental_vehicles")
day_stat_reservation_count = pd.merge(reservations, rental_vehicles[['reservation_id','vehicle_id','pickup_station_id']], how='left', on='reservation_id')
print("row count of merged above: ",len(day_stat_reservation_count))
#reservation join rental vehilce inner join location 

print("join location")
day_stat_reservation_count = pd.merge(day_stat_reservation_count, location[["pickup_station_id","name"]], how='inner',on='pickup_station_id')
print("row count of merged above: ",len(day_stat_reservation_count))

print("join vehicles")
day_stat_reservation_count = pd.merge(day_stat_reservation_count, vehicles1[["vehicle_id","model_id"]], how="inner", on="vehicle_id")
print("row count of merged above: ",len(day_stat_reservation_count))
day_stat_reservation_count["vehicle_model_id"] = day_stat_reservation_count["model_id"]

print("join vehicle model")
day_stat_reservation_count = pd.merge(day_stat_reservation_count, vehicle_models1[["vehicle_model_id","standard_model_id","vehicle_model_name"]], how="inner", on="vehicle_model_id")
print("row count of merged above: ",len(day_stat_reservation_count))

print("join standard vehicle model")
day_stat_reservation_count = pd.merge(day_stat_reservation_count, vehicle_models2[["vehicle_model_name","standard_model_id"]], how="inner", on="standard_model_id")
print("row count of merged above: ",len(day_stat_reservation_count))

print("join mc(chargevheicle and charge)")
day_stat_reservation_count = pd.merge(day_stat_reservation_count, vehicle_charge, how="inner", on='vehicle_id')
print("row count of merged above: ",len(day_stat_reservation_count))
print("join reservated_at")
day_stat_reservation_count = pd.merge(day_stat_reservation_count, reservated_at, how="inner", on="reservation_id")
print("row count of merged above: ",len(day_stat_reservation_count))
# print("join mp")
# day_stat_reservation_count = pd.merge(day_stat_reservation_count, mp, how="left", on='reservation_id')
# print("row count of merged above: ",len(day_stat_reservation_count))
print()


# In[27]:


# 비교를 위한 컬럼 type 변경
day_stat_reservation_count["reserveated_at"] = pd.to_datetime(day_stat_reservation_count['reservated_at'])


# In[28]:


#where 절

day_stat_reservation_count = day_stat_reservation_count[day_stat_reservation_count['reservated_at'] >= datetime.datetime.strptime('2020-09-20 22:00:00','%Y-%m-%d %H:%M:%S')]
day_stat_reservation_count = day_stat_reservation_count[day_stat_reservation_count['reservated_at'] <= datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')]
day_stat_reservation_count = day_stat_reservation_count[day_stat_reservation_count['deleted_at'].isna()]
day_stat_reservation_count = day_stat_reservation_count[day_stat_reservation_count['reservation_status'].isin(('CONFIRMED', 'INUSE', 'RETURNED'))]


# In[ ]:





# In[29]:


#데이터 전처리 진행
day_stat_reservation_count = day_stat_reservation_count[day_stat_reservation_count['deleted_at'].isna()]
day_stat_reservation_count = day_stat_reservation_count.sort_values(by="reservated_at", ascending=False)
day_stat_reservation_count.reservation_start_time = day_stat_reservation_count.reservation_start_time.dt.date
day_stat_reservation_count.reservation_end_time  = day_stat_reservation_count.reservation_end_time.dt.date
day_stat_reservation_count["rental days"]= day_stat_reservation_count.reservation_end_time - day_stat_reservation_count.reservation_start_time
# day_stat_reservation_count = day_stat_reservation_count.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)


# # 일자 별 예약일 합계 

# In[30]:


print("1~7 days")
week = day_stat_reservation_count[(day_stat_reservation_count["rental days"]>=datetime.timedelta(days=0)) & (day_stat_reservation_count["rental days"]<= datetime.timedelta(days=6)) ].id.count()
print(week)
print("====================")
print("8~29 days")
month = day_stat_reservation_count[(day_stat_reservation_count["rental days"]>=datetime.timedelta(days=7)) & (day_stat_reservation_count["rental days"]<= datetime.timedelta(days=29)) ].id.count()
print(month)
print("====================")
print("1~3 months")
three_month = day_stat_reservation_count[(day_stat_reservation_count["rental days"]>=datetime.timedelta(days=30)) & (day_stat_reservation_count["rental days"]<= datetime.timedelta(days=89)) ].id.count()
print(three_month)
print("====================")
print("3~ months")
etc = day_stat_reservation_count[(day_stat_reservation_count["rental days"]>=datetime.timedelta(days=90))  ].id.count()
print(etc)
print("====================")
print("all")
print(len(day_stat_reservation_count))
day_reservation = {'~1week': [week], 

                  '~1M': [month], 

                 '1M~3M': [three_month],
                  
                  '3M~': [etc],
                  'SUM':[len(day_stat_reservation_count)]}
df = pd.DataFrame(day_reservation)


# # 차종별 예약 현황 - 국가별 신규 차량이 등록되고 있으므로 전과 데이터 상이 할 수 있음

# In[31]:


print('--------------------------------------------------------')
print("차종별 예약 현황")
print("차종별 예약현황 data processing:")
print("left join reservations, rental_vhehicles")
vehicle_stat_reservation_count = pd.merge(reservations, rental_vehicles[["reservation_id","vehicle_id","pickup_station_id"]], how="left", on="reservation_id")
print("row count of merged above: ",len(vehicle_stat_reservation_count))

print("inner join location")
vehicle_stat_reservation_count = pd.merge(vehicle_stat_reservation_count, location[["pickup_station_id","name"]], how='inner',on='pickup_station_id')
print("row count of merged above: ",len(vehicle_stat_reservation_count))

print("inner join vehicles")
vehicle_stat_reservation_count = pd.merge(vehicle_stat_reservation_count, vehicles1[["vehicle_id","model_id"]], how="inner", on="vehicle_id")
vehicle_stat_reservation_count["vehicle_model_id"] = vehicle_stat_reservation_count["model_id"]
print("row count of merged above: ",len(vehicle_stat_reservation_count))
print("inner join vehicle models")

vehicle_stat_reservation_count2 = pd.merge(vehicle_stat_reservation_count, vehicle_models1[["vehicle_model_id","standard_model_id","vehicle_model_name"]], how="inner", on="vehicle_model_id")
print("row count of merged above: ",len(vehicle_stat_reservation_count2))

print("inner join standard vehicle models")
vehicle_stat_reservation_count2 = pd.merge(vehicle_stat_reservation_count2, vehicle_models2[["vehicle_model_name","standard_model_id"]], how="inner", on="standard_model_id")
print("row count of merged above: ",len(vehicle_stat_reservation_count2))

print("inner join mc(vehicle charge and charge)")
vehicle_stat_reservation_count2 = pd.merge(vehicle_stat_reservation_count2, vehicle_charge, how="inner", on="vehicle_id")
print("row count of merged above: ",len(vehicle_stat_reservation_count2))

print("inner join reservated_at")
vehicle_stat_reservation_count2 = pd.merge(vehicle_stat_reservation_count2, reservated_at, how="inner", on="reservation_id")
print("row count of merged above: ",len(vehicle_stat_reservation_count2))
print()


# In[32]:


#where 절
vehicle_stat_reservation_count2 = vehicle_stat_reservation_count2[vehicle_stat_reservation_count2['reservated_at'] >= datetime.datetime.strptime('2020-09-20 22:00:00','%Y-%m-%d %H:%M:%S')]
vehicle_stat_reservation_count2 = vehicle_stat_reservation_count2[vehicle_stat_reservation_count2['reservated_at'] <= datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')]
vehicle_stat_reservation_count2 = vehicle_stat_reservation_count2[vehicle_stat_reservation_count2['deleted_at'].isna()]
vehicle_stat_reservation_count2 = vehicle_stat_reservation_count2[vehicle_stat_reservation_count2['reservation_status'].isin(('CONFIRMED', 'INUSE', 'RETURNED'))]


# In[33]:


vehicle_stat_reservation_count2 = vehicle_stat_reservation_count2.sort_values(by="reservated_at", ascending=False)
# vehicle_stat_reservation_count2 = vehicle_stat_reservation_count2.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)


# In[34]:


vehicle_stat_reservation_count2["this week count"]=np.where((vehicle_stat_reservation_count2["reservated_at"] >= datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')) &( vehicle_stat_reservation_count2["reservated_at"] <= datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')), 1, 0)
vehicle_stat_reservation_count2["tot count"] =1


# # 차종별 예약 현황

# In[35]:


vehicle_stat_reservation_count2_group = vehicle_stat_reservation_count2[["this week count","tot count"]].groupby(vehicle_stat_reservation_count2['vehicle_model_name_y'])
print("reservation count group by vehicle model")# 국가 별로 나눠야함 --> business country join해서 groupby에 country_name 추가
print(vehicle_stat_reservation_count2_group.sum())
print()


# # 차량 등록 현황

# ### 이부분은 원래 가용차량 아닌 전체 차량이지만 그냥 스킵합시다 요즘제 등록안된

# # 국가별 차량 등록 현황

# In[ ]:





# In[36]:


print('--------------------------------------------------------')
print("국가별 차량 등록 현황")
print("국가별 차량 등록 현황 data processing:")
print("left join business, location")
business["business_id"] = business["id"]
country_vehicle = pd.merge(business, location, how="left",on="business_id")
print("row count of merged above: ",len(country_vehicle))

print("left join vehicles")
vehicles["business_id"] = vehicles["group_id"]
country_vehicle = pd.merge(country_vehicle, vehicles, how="left", left_on=["business_id","pickup_station_id"], right_on=["business_id", "station_id"])
print("row count of merged above: ",len(country_vehicle))

print("left join vehicle models")
country_vehicle["vehicle_model_id"] = country_vehicle["model_id"]
country_vehicle = pd.merge(country_vehicle, vehicle_models[["vehicle_model_id","standard_model_id","vehicle_model_name"]], how="inner", on="vehicle_model_id")
print("row count of merged above: ",len(country_vehicle))

print("left join standard vehicle model")
country_vehicle = pd.merge(country_vehicle, vehicle_models2[["vehicle_model_name","standard_model_id"]], how="inner", on="standard_model_id")
print("row count of merged above: ",len(country_vehicle))

print("left join country")
country_vehicle = pd.merge(country_vehicle, country[["id","country_name"]], how="inner", left_on=["country_id"], right_on=["id"] )
print("row count of merged above: ",len(country_vehicle))
country_vehicle = country_vehicle.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)


# In[37]:


country_vehicle = country_vehicle[
    (country_vehicle.deleted_at.isna()) &
    (country_vehicle.deleted_at_x.isna()) &
    (country_vehicle.deleted_at_y.isna())
]


# In[38]:


print("")
print("국가, 모델(standard) 별 차량 등록 현황")
print(country_vehicle[['country_name', 'vehicle_model_name_y','id_x']].groupby(['country_name', 'vehicle_model_name_y']).count())


# In[39]:


print("국가, 딜러그룹, 딜러쉽 별 차량 등록 현황")
print(country_vehicle[['country_name', 'name_x','created_at_x','name_y','created_at_y','id_x']].groupby(['country_name', 'name_x','created_at_x','name_y','created_at_y']).count())


# # MP online offline

# In[40]:


print('--------------------------------------------------------')
print("결제 방법 통계(Online, Offline")
print("on-off line data processing:")
print("left join reservations, business")
mp_reservations = reservations[reservations["deleted_at"].isna()]
mp_reservations = pd.merge(mp_reservations, business[['name','business_id']], how='inner',left_on="group_id", right_on='business_id')
print("row count of merged above: ",len(mp_reservations))

print("left join maasfms payment")
mp_reservations = pd.merge(mp_reservations, mp, how='left',on="reservation_id")
print("row count of merged above: ",len(mp_reservations))

print("left join reservated_at")
mp_reservations = pd.merge(mp_reservations, reservated_at, how='left',on='reservation_id' )
print("row count of merged above: ",len(mp_reservations))


# In[41]:


#where 절
mp_reservations = mp_reservations[mp_reservations['created_at'] >= datetime.datetime.strptime('2020-09-20 22:00:00','%Y-%m-%d %H:%M:%S')]
mp_reservations = mp_reservations[mp_reservations['reservation_status'].isin(('CONFIRMED', 'INUSE', 'RETURNED'))]
mp_reservations = mp_reservations[mp_reservations['reservated_at'] >= datetime.datetime.strptime(start_date,'%Y-%m-%d %H:%M:%S')]
mp_reservations = mp_reservations[mp_reservations['reservated_at'] <= datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')]


# In[42]:


mp_reservations = mp_reservations.drop_duplicates(subset=["reservation_id"], keep="last")
mp_reservations.on_off = mp_reservations.on_off.fillna('Offline')
print()
print("결제 방법 별 수")
print(mp_reservations[["on_off","reservation_id"]].groupby('on_off').count())


# In[43]:


print(mp_reservations[["on_off","reservation_id"]])


# # 예약 종류별 통계

# In[49]:


print('--------------------------------------------------------')
print("예약 종류별 통계")
print("reservation type data processing:")
print("join reservations, rental_vehicles")
regist_type_stat = pd.merge(reservations, rental_vehicles[['reservation_id','vehicle_id','pickup_station_id']], how='left', on='reservation_id')
print("row count of merged above: ",len(regist_type_stat))
#reservation join rental vehilce inner join location 

print("join location")
regist_type_stat = pd.merge(regist_type_stat, location[["pickup_station_id","name"]], how='inner',on='pickup_station_id')
print("row count of merged above: ",len(regist_type_stat))

print("join vehicles")
regist_type_stat = pd.merge(regist_type_stat, vehicles1[["vehicle_id","model_id"]], how="inner", on="vehicle_id")
print("row count of merged above: ",len(regist_type_stat))
regist_type_stat["vehicle_model_id"] = regist_type_stat["model_id"]

print("join vehicle model")
regist_type_stat = pd.merge(regist_type_stat, vehicle_models1[["vehicle_model_id","standard_model_id","vehicle_model_name"]], how="inner", on="vehicle_model_id")
print("row count of merged above: ",len(regist_type_stat))

print("join standard vehicle model")
regist_type_stat = pd.merge(regist_type_stat, vehicle_models2[["vehicle_model_name","standard_model_id"]], how="inner", on="standard_model_id")
print("row count of merged above: ",len(regist_type_stat))

print("join mc(chargevheicle and charge)")
regist_type_stat = pd.merge(regist_type_stat, vehicle_charge, how="inner", on='vehicle_id')
print("row count of merged above: ",len(regist_type_stat))

print("join reservation_short")
reservation_short = reservation_short[reservation_short['deleted_at'].isna()]
regist_type_stat = pd.merge(regist_type_stat, reservation_short, how="left", on="reservation_id")
print("row count of merged above:", len(regist_type_stat))

print("join reservated_at")
regist_type_stat = pd.merge(regist_type_stat, reservated_at, how="inner", on="reservation_id")
print("row count of merged above: ",len(regist_type_stat))


# In[53]:


regist_type_stat


# In[55]:


#where 절
regist_type_stat = regist_type_stat[regist_type_stat['deleted_at_x'].isna()]
regist_type_stat = regist_type_stat[regist_type_stat['reservation_status_x'].isin(('CONFIRMED', 'INUSE', 'RETURNED'))]
regist_type_stat = regist_type_stat[regist_type_stat['reservated_at'] >= datetime.datetime.strptime('2020-09-20 22:00:00','%Y-%m-%d %H:%M:%S')]
regist_type_stat = regist_type_stat[regist_type_stat['reservated_at'] <= datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')]


# In[57]:


regist_type_stat[["reservation_id","reservation_regist_type","reservated_at"]].groupby('reservation_regist_type').count()


# # 가동률은

# In[53]:


print('--------------------------------------------------------')
print("가동률")
activations = reservations[reservations['reservation_end_time'] >= reservations['reservation_start_time']]
activations = activations[activations['reservation_status'].isin(('CONFIRMED', 'INUSE', 'RETURNED'))]
activations = activations[activations["deleted_at"].isna()]                           
activations = activations[activations['reservation_start_time'] <= datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')]
activations = activations[activations['reservation_end_time'] >= datetime.datetime.strptime(start_date,'%Y-%m-%d %H:%M:%S')]


# In[54]:


activations['activations_start'] = ''
activations['activations_end'] = ''
for i in range(len(activations['reservation_start_time'])):
    if activations['reservation_start_time'].iloc[i]<= datetime.datetime.strptime(start_date,'%Y-%m-%d %H:%M:%S'):
        activations['activations_start'].iloc[i] = datetime.datetime.strptime(start_date,'%Y-%m-%d %H:%M:%S')
    else: 
        activations['activations_start'].iloc[i] = activations['reservation_start_time'].iloc[i]
        
    if activations['reservation_end_time'].iloc[i] > datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S'):
        activations['activations_end'].iloc[i] = datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')
    else: 
        activations['activations_end'].iloc[i] = activations['reservation_end_time'].iloc[i]
        
        
activations.activations_start = pd.to_datetime(activations.activations_start)
activations.activations_end  = pd.to_datetime(activations.activations_end)
activations["rental days"]= (activations.activations_end - activations.activations_start).dt.days + 1
activation_calculate = pd.DataFrame({
    "총 예약일 수" : [activations["rental days"].sum()],
})
activation_calculate


# In[ ]:





# # 파일로

# In[46]:


print("========================== data processing end extracting to excel file ==========================")
print("extracting data to excel")
today = strftime("%Y-%m-%d", gmtime())
name = "주1회 보고용 통계 " + today

writer = pd.ExcelWriter(name+".xlsx", engine = 'xlsxwriter')
df.to_excel(writer, sheet_name="day_stat_reservation_count")
vehicle_stat_reservation_count2_group.sum().to_excel(writer, sheet_name="차종별 등록 현황")
country_vehicle[['country_name', 'vehicle_model_name_y','id_x']].groupby(['country_name', 'vehicle_model_name_y']).count().to_excel(writer, sheet_name="차량 등록 현황")
country_vehicle[['country_name', 'name_x','created_at_x','name_y','created_at_y','id_x']].groupby(['country_name', 'name_x','created_at_x','name_y','created_at_y']).count().to_excel(writer, sheet_name="차량 등록 현황2")
mp_reservations[["on_off","reservation_id"]].groupby('on_off').count().to_excel(writer, sheet_name="mp_on_off")
regist_type_stat[["reservation_id","reservation_regist_type","reservated_at"]].groupby('reservation_regist_type').count().to_excel(writer, sheet_name="regist_type")
activation_calculate.to_excel(writer, sheet_name="가동률 예약일 수")

writer.save()
writer.close()


# In[ ]:




