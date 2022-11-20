from sqlite_helper import *
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

# Create Airport Codes Table


# Create Flights Table
create_flights_sql = '''Create Table Flights (
    FlightDate DATE,
    Airline TEXT, 'Origin', 'Dest', 'Cancelled', 'Diverted', 'CRSDepTime', 'DepTime', 'DepDelayMinutes', 'DepDelay', 'ArrTime', 'ArrDelayMinutes', 'AirTime', 'CRSElapsedTime', 'ActualElapsedTime', 'Distance', 'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'Marketing_Airline_Network', 'Operated_or_Branded_Code_Share_Partners', 'DOT_ID_Marketing_Airline', 'IATA_Code_Marketing_Airline', 'Flight_Number_Marketing_Airline', 'Operating_Airline', 'DOT_ID_Operating_Airline', 'IATA_Code_Operating_Airline', 'Tail_Number', 'Flight_Number_Operating_Airline', 'OriginAirportID', 'OriginAirportSeqID', 'OriginCityMarketID', 'OriginCityName', 'OriginState', 'OriginStateFips', 'OriginStateName', 'OriginWac', 'DestAirportID', 'DestAirportSeqID', 'DestCityMarketID', 'DestCityName', 'DestState', 'DestStateFips', 'DestStateName', 'DestWac', 'DepDel15', 'DepartureDelayGroups', 'DepTimeBlk', 'TaxiOut', 'WheelsOff', 'WheelsOn', 'TaxiIn', 'CRSArrTime', 'ArrDelay', 'ArrDel15', 'ArrivalDelayGroups', 'ArrTimeBlk', 'DistanceGroup', 'DivAirportLandings']
)
'''