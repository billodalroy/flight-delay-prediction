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
create_flights_sql = '''CREATE TABLE Flights (
    FlightDate DATE,
    Airline TEXT, 
    Origin TEXT, 
    Dest TEXT, 
    Cancelled TEXT, 
    Diverted TEXT, 
    CRSDepTime INTEGER, 
    DepTime INTEGER, 
    DepDelayMinutes INTEGER, 
    DepDelay INTEGER, 
    ArrTime INTEGER, 
    ArrDelayMinutes INTEGER, 
    AirTime INTEGER, 
    CRSElapsedTime INTEGER, 
    ActualElapsedTime INTEGER, 
    Distance INTEGER, 
    Year INTEGER, 
    Quarter INTEGER, 
    Month INTEGER, 
    DayofMonth TEXT, 
    DayOfWeek TEXT, 
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
    DepDel15 TEXT, 
    DepartureDelayGroups TEXT, 
    DepTimeBlk TEXT, 
    TaxiOut TEXT, 
    WheelsOff TEXT, 
    WheelsOn TEXT, 
    TaxiIn TEXT, 
    CRSArrTime TEXT, 
    ArrDelay TEXT, 
    ArrDel15 TEXT, 
    ArrivalDelayGroups TEXT, 
    ArrTimeBlk TEXT, 
    DistanceGroup TEXT, 
    DivAirportLandings TEXT
)
'''

    ### NOtes: remove all other airlines other than US 4

