from sqlite_helper import *

#Connection
conn_1 = create_connection("main.db")

# Create Airports Table
create_airports_sql = '''CREATE TABLE airports (
    Airport TEXT PRIMARY KEY,
    City TEXT,
    State TEXT
)
'''
create_table(conn_1,create_airports_sql)

# Selecting Unique Airports
airports_sql_statement = '''SELECT DISTINCT Origin Airport,SUBSTR(OriginCityName,1,INSTR(OriginCityName,',')-1) City,OriginState State
FROM raw_data
UNION
SELECT DISTINCT Dest Airport,SUBSTR(DestCityName,1,INSTR(DestCityName,',')-1) City,DestState State
FROM raw_data
'''

#Executing Query
airports = execute_sql_statement(airports_sql_statement, conn_1)

# Pushing to New Table
with conn_1:
    for airport in airports:
        insert_airport(conn_1,airport)


# Create Routes Table
creates_routes_table_sql = """
CREATE TABLE routes (
    RouteID INTEGER PRIMARY KEY AUTOINCREMENT,
    Source TEXT,
    Destination TEXT
    FOREIGN KEY(Source) REFERENCES airports(Airport)
    FOREIGN KEY(Destination) REFERENCES airports(Airport)
)"""

create_table(conn_1,creates_routes_table_sql)

select_routes_sql = """SELECT Origin, Dest
FROM raw_data
"""

routes_with_duplicate = execute_sql_statement(select_routes_sql, conn_1)

unique_routes = set()

# Capturing Unique Routes in the data
for route in routes_with_duplicate:
    unique_routes.add(route)

# Adding into routes table
with conn_1:
    for route in unique_routes:
        insert_route(conn_1, route)


create_normalized_flights_sql = """CREATE TABLE flights (
    RecordID INTEGER PRIMARY KEY AUTOINCREMENT,
    FlightDate DATE, 
    RouteID INTEGER, 
    Cancelled TEXT, 
    Diverted TEXT, 
    CRSDepTime TEXT, 
    DepTime TEXT, 
    DepDelayMinutes REAL, 
    DepDelay REAL, 
    ArrTime TEXT, 
    ArrDelayMinutes REAL, 
    AirTime REAL,
    Distance REAL,
    IATA_Code_Marketing_Airline TEXT,
    Flight_Number_Operating_Airline TEXT,
    FOREIGN KEY(RouteID) REFERENCES routes(RouteID),
    FOREIGN KEY(IATA_Code_Marketing_Airline) REFERENCES airlines(Code)
)
"""

create_table(conn_1,create_normalized_flights_sql)

# insert into normalized flights table
select_flights_sql = """SELECT FlightDate,
    Origin,
    Dest, 
    Cancelled, 
    Diverted, 
    CRSDepTime, 
    DepTime, 
    DepDelayMinutes, 
    DepDelay, 
    ArrTime, 
    ArrDelayMinutes, 
    AirTime,
    Distance,
    IATA_Code_Marketing_Airline,
    Flight_Number_Operating_Airline
    FROM raw_data
"""
flights_truncated = execute_sql_statement(select_flights_sql, conn_1)

# CREATING ROUTES MAPPING
select_all_routes_sql = """SELECT Source, Destination, RouteID
FROM routes"""
routes = execute_sql_statement(select_all_routes_sql, conn_1)
route_mapping = {}
for route in routes:
    route_mapping[(route[0],route[1])] = route[2]

# print(route_mapping)
# Inserting Data into normalized flights table
with conn_1:
    for record in flights_truncated:
        new_record = []
        new_record.append(record[0])
        new_record.append(route_mapping[(record[1], record[2])])
        for r in record[3:]:
            new_record.append(r)
        # print(new_record)
        insert_flight(conn_1, tuple(new_record))
 