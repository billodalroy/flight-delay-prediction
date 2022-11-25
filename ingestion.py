from sqlite_helper import *
import csv

# Create Connection to DB
conn_1 = create_connection("main.db")

# Create Airlines Table
create_airlines_sql = '''CREATE TABLE airlines (
    Code TEXT PRIMARY KEY,
    Description TEXT
)
'''

create_table(conn_1, create_airlines_sql)

# Adding Rows
header = None
with conn_1:
    with open("Airlines.csv") as file:
        for line in file:
            if not header:
                header = line.strip().split(',')
                print("Header", header)
                continue
            values = line.strip().split(',',1)
            #print(values)
            rid = insert_airline(conn_1,values)
            #print(rid)

# Create Flights Table
create_flights_sql = '''CREATE TABLE Flights (
    FlightDate DATE,
    Airline TEXT, 
    Origin TEXT, 
    Dest TEXT, 
    Cancelled TEXT, 
    Diverted TEXT, 
    CRSDepTime TEXT, 
    DepTime TEXT, 
    DepDelayMinutes REAL, 
    DepDelay REAL, 
    ArrTime TEXT, 
    ArrDelayMinutes REAL, 
    AirTime REAL, 
    CRSElapsedTime REAL, 
    ActualElapsedTime INTEGER, 
    Distance REAL, 
    Year INTEGER, 
    Quarter INTEGER, 
    Month INTEGER, 
    DayofMonth INTEGER, 
    DayOfWeek INTEGER, 
    Marketing_Airline_Network TEXT, 
    Operated_or_Branded_Code_Share_Partners TEXT, 
    DOT_ID_Marketing_Airline INTEGER, 
    IATA_Code_Marketing_Airline TEXT, 
    Flight_Number_Marketing_Airline TEXT, 
    Operating_Airline TEXT, 
    DOT_ID_Operating_Airline TEXT,
    IATA_Code_Operating_Airline TEXT, 
    Tail_Number TEXT, 
    Flight_Number_Operating_Airline TEXT, 
    OriginAirportID TEXT, 
    OriginAirportSeqID TEXT, 
    OriginCityMarketID TEXT, 
    OriginCityName TEXT, 
    OriginState TEXT, 
    OriginStateFips TEXT, 
    OriginStateName TEXT, 
    OriginWac TEXT, 
    DestAirportID TEXT, 
    DestAirportSeqID TEXT, 
    DestCityMarketID TEXT, 
    DestCityName TEXT, 
    DestState TEXT, 
    DestStateFips TEXT, 
    DestStateName TEXT, 
    DestWac TEXT, 
    DepDel15 REAL, 
    DepartureDelayGroups REAL, 
    DepTimeBlk TEXT, 
    TaxiOut REAL, 
    WheelsOff REAL, 
    WheelsOn REAL, 
    TaxiIn REAL, 
    CRSArrTime INTEGER, 
    ArrDelay REAL, 
    ArrDel15 TEXT,
    ArrivalDelayGroups TEXT, 
    ArrTimeBlk TEXT, 
    DistanceGroup TEXT,
    DivAirportLandings TEXT
)
'''

# create_table(conn_1, create_flights_sql)
print("\n Table Created")


header = None
with conn_1:
    with open("Combined_Flights_2021.csv") as file:
        values = csv.reader(file)
        for row in values:
            if not header:
                header = 1
                continue
            # print(row)
            # print(values) 
            rid = insert_master_record(conn_1,tuple(row))
            # print(rid)
conn_1.close()

conn_1 = create_connection("main.db")

header = None
with conn_1:
    with open("Combined_Flights_2022.csv") as file:
        values = csv.reader(file)
        for row in values:
            if not header:
                header = 1
                continue
            # print(row)
            # print(values) 
            rid = insert_master_record(conn_1,tuple(row))
            # print(rid)
conn_1.close()

