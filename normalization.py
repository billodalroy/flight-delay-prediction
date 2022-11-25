from sqlite_helper import *

#Connection
conn_1 = create_connection("main.db")

# Create Airports Table
create_airports_sql = '''CREATE TABLE Airports (
    Airport TEXT PRIMARY KEY,
    City TEXT,
    State TEXT
)
'''
create_table(conn_1,create_airports_sql)

# Selecting Unique Airports
airports_sql_statement = '''SELECT DISTINCT Origin Airport,SUBSTR(OriginCityName,1,INSTR(OriginCityName,',')-1) City,OriginState State
FROM Flights
UNION
SELECT DISTINCT Dest Airport,SUBSTR(DestCityName,1,INSTR(DestCityName,',')-1) City,DestState State
FROM Flights
'''

#Executing Query
airports = execute_sql_statement(airports_sql_statement, conn_1)

# Pushing to New Table
with conn_1:
    for airport in airports:
        insert_airport(conn_1,airport)

