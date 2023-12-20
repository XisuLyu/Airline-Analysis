# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 14:39:26 2023

@author: s
"""
"""Objective
#The goal of this data analysis project using sql would be to 
#identify opportunities to increase the occupancy rate on low-performing flights,
#which can ultimately lead to increased profitability for the airline
"""

"""
Importing Libraries

"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')


"""Data Base Connection"""
travel = 'C:/Users/s/bikeshare/Airlines Data Analysis/travel.sqlite'
connection = sqlite3.connect(travel)
cursor = connection.cursor()

cursor.execute("""Select name from sqlite_master where type = 'table';""")
print('List of tables present in the database')
table_list = [table[0] 
              for table in cursor.fetchall()] 
#create a list of table names and retrive all table name to this list through for cycle
table_list

"""'aircrafts_data',#飞机数据
 'airports_data',#机场数据
 'boarding_passes',#登机牌
 'bookings',#预订
 'flights',#航班
 'seats',#座位
 'ticket_flights',#机票——航班
 'tickets#机票'"""
 
 
"""Data Exploration"""
#飞机数据表-该表有三列104行-飞机代码，飞机型号和飞行范围（？
#aircrafts_date:aircraft_code,model,range

aircrafts_data = pd.read_sql_query("select * from aircrafts_data", connection)
aircrafts_data.head()
aircrafts_data

#机场数据表-该表有5列104行-机场代码，机场名，（机场所属）的城市，坐标和时区
#airports_data:airport_code, airport_name,city, coordinates, timezone
airports_data = pd.read_sql_query("select * from airports_data", connection)
airports_data.head()
airports_data
for airport_col in airports_data.columns:
    print(airport_col)
"""airport_code
airport_name
city
coordinates
timezone"""

#登机牌表-4列57万行-机票号码，飞行代码，登机号，座位号
#boarding_passes:ticket_no,flight_id,boarding_no,seat_no
boarding_passes = pd.read_sql_query("select * from boarding_passes", connection)
boarding_passes

#预订表-3列 26万行-预订编号，预订日期，预订总金额
#bookings:book_ref,book_date,tatol_amount
bookings = pd.read_sql_query("select * from bookings", connection)
bookings

#flights:flight_id,flight_no,scheduled_departure,scheduled_arrival,departure_airport
#arrival_airport,status,aircraft_code,actual_departure,actual_arrival
#航班：航班 ID、航班号、预定出发、预定到达、出发机场、到达机场、状态、飞机代码、实际出发、实际到达
flights = pd.read_sql_query("select * from flights", connection)
flights
for flights_col in flights.columns:
    print(flights_col)

#seats:aircraft_code,seat_no,fare_condition
#座位表:飞机代码、座位号、票价条件
seats = pd.read_sql_query("select * from seats", connection)
seats

#ticket_flights:ticket_no,flight_id, fare_conditions,amount
#机票航班：机票编号、航班 ID、票价条件、金额
ticket_flights = pd.read_sql_query("select * from ticket_flights", connection)
ticket_flights

#机票:票号，预订编号，旅客 ID
#tickets:ticket_no,book_ref,passenger_id
tickets = pd.read_sql_query("select * from tickets", connection)
tickets

#format table 
for table in table_list:
    print ('\ntable:',table)
    column_info = connection.execute("PRAGMA table_info({})".format(table))
    for column in column_info.fetchall():
        print(column[1:3])
        
#check missing value
for table in table_list:
    print('\ntable,',table)
    df_table = pd.read_sql_query(f"select * from {table}", connection)
    print(df_table.isnull().sum())
    
"""Basic Analysis"""
#How many planes have more than 100 seats
#有多少飞机有多于100个以上的座位-计算上座率
#在座位表中，每一架飞机座位号码对应的aircraft_code出现的次数即是该飞机的座位总数，
pd.read_sql_query("""select aircraft_code, count(*) as num_seats from seats
                  group by aircraft_code""",connection)
                  
pd.read_sql_query("""select aircraft_code, count(*) as num_seats from seats 
                  group by aircraft_code having num_seats > 100""",connection)


#How the number of tickets boooked and total amount earned changed with the time
#订票数量和订票金额如何随着时间的变化而改变
#tickets_1 = pd.read_sql_query("""select ticket_no, total_amount, book_date from tickets inner join bookings
#                  where tickets.book_ref = bookings.book_ref
#                  order by book_date""",connection)
#pd.read_sql_query(""" select ticket_no, count(*) as num_tickets from tickets_1
#              group by total_amount""",connection)

tickets_2 = pd.read_sql_query(""" select * from tickets inner join bookings
                  on tickets.book_ref = bookings.book_ref""",connection)
#change book_date values from object type to datetime type
tickets_2['book_date'] = pd.to_datetime(tickets_2['book_date'])
tickets_2.dtypes
#extract exact times from book_date but only keep the date
tickets_2['date'] = tickets_2['book_date'].dt.date

#以日期为索引聚合groupby,并根据出现日期的次数分别count计算相应日期的票数
#得出随时间变化的订票数量
x = tickets_2.groupby('date')[['date']].count()  
plt.figure(figsize=(18,6))
plt.plot(x.index, x['date'],marker = '^')         
plt.xlabel('Date',fontsize = 20)
plt.ylabel("Number of Tickets", fontsize = 20)
plt.grid('b')
plt.show()

bookings = pd.read_sql_query("select * from bookings", connection)
bookings['book_date'] = pd.to_datetime(bookings['book_date'])
bookings['date'] = bookings['book_date'].dt.date
#以日期为索引聚合groupby,并根据出现日期的次数分别将相应日期的收入相加
#得出随时间变化的订票金额
y = bookings.groupby('date')[['total_amount']].sum()
y
plt.figure(figsize=(18,6))
plt.plot(y.index, y['total_amount'],marker = '^')         
plt.xlabel('Date',fontsize = 20)
plt.ylabel("Total amount earned", fontsize = 20)
plt.grid('b')
plt.show()
                             
#Calculate the average charges for each aircraft with different fare conditions
#计算不同舱位下每架飞机的平均花费（飞机成本）
df = pd.read_sql_query("""select fare_conditions, aircraft_code, avg(amount)
                       from ticket_flights join flights 
                       on ticket_flights.flight_id = flights.flight_id
                       group by aircraft_code, fare_conditions""", connection)
df
sns.barplot(data = df, x = 'aircraft_code', y = 'avg(amount)', hue = 'fare_conditions')

"""Analyzing occupancy rate"""
#For each aircraft, calculate the total revenue per year and the average revenue per ticket
#对于每架飞机，计算每年的总收入和每张机票的平均收入
pd.read_sql_query("""select aircraft_code, ticket_count, total_revenue, total_revenue/ticket_count as avg_recenue_per_ticket
                  from
                  (select aircraft_code,count(*) as ticket_count,sum(amount) as total_revenue 
                  from 
                  ticket_flights join flights 
                  on 
                  ticket_flights.flight_id = flights.flight_id
                  group by aircraft_code)""", 
                  connection)

#
#Calculate the average occupancy per aircraft
#计算每架飞机的平均上座率-boarding pass table
#pd.read_sql_query(""" select ticket_no,count(*) as seat_occupy/count(seat_no) 
#                 from
#                 ticket_flights.fare_conditions = seats.fare_conditions
#                  group by aircraft_code """, connection)

pd.read_sql_query("""select aircraft_code,flights.flight_id, count(*) as seats_count
                  from boarding_passes inner join flights
                  on boarding_passes.flight_id = flights.flight_id
                  group by aircraft_code,flights.flight_id""",connection)
                  
pd.read_sql_query("""select aircraft_code,count(*) as num_seats
                  from seats group by aircraft_code""",connection)
                  
occupancy_rate = pd.read_sql_query("""select a.aircraft_code, avg(a.seats_count) as booked_seats, 
                  b.num_seats,avg(a.seats_count)/b.num_seats as occupancy_rate
                  from(select aircraft_code,flights.flight_id, count(*) as seats_count
                                    from boarding_passes inner join flights
                                    on boarding_passes.flight_id = flights.flight_id
                                    group by aircraft_code,flights.flight_id) as a
                    inner join
                    (select aircraft_code,count(*) as num_seats from seats group by aircraft_code)as b
                    on a.aircraft_code = b.aircraft_code 
                    group by a.aircraft_code""", connection)
occupancy_rate


#Calculate by how much the total annual turnover could increase by giving all aircraft a 10% higher occupancy rate
#计算如果让所有飞机的上座率提高10%，每年的总营业额可以增加多少
occupancy_rate['Inc occupancy rate'] = occupancy_rate['occupancy_rate'] + occupancy_rate['occupancy_rate']*0.1
occupancy_rate

pd.set_option("display.float_format",str)
total_revenue = pd.read_sql_query("""select aircraft_code,sum(amount) as total_revenue from ticket_flights
                                  join flights on ticket_flights.flight_id = flights.flight_id
                                  group by aircraft_code""", connection)
occupancy_rate['Inc Total Annual Turnover'] = (total_revenue['total_revenue']/occupancy_rate['occupancy_rate']*occupancy_rate['Inc occupancy rate'])
occupancy_rate   














