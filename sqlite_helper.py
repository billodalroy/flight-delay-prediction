import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def execute_sql_statement(sql_statement, conn):
    try:
        cur = conn.cursor()
        cur.execute(sql_statement)
    except Error as e:
        print(e)
    rows = cur.fetchall()

    return rows

def insert_airline(conn, values):
    sql = '''INSERT INTO Airlines(Code, Description) VALUES(?,?)'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
    except Error as e:
        print(e)
    return cur.lastrowid

def insert_master_record(conn, values):
    sql = '''INSERT INTO Flights(
    FlightDate,
    Airline, 
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
    CRSElapsedTime, 
    ActualElapsedTime, 
    Distance, 
    Year, 
    Quarter, 
    Month, 
    DayofMonth, 
    DayOfWeek, 
    Marketing_Airline_Network, 
    Operated_or_Branded_Code_Share_Partners, 
    DOT_ID_Marketing_Airline, 
    IATA_Code_Marketing_Airline, 
    Flight_Number_Marketing_Airline, 
    Operating_Airline, 
    DOT_ID_Operating_Airline,
    IATA_Code_Operating_Airline, 
    Tail_Number, 
    Flight_Number_Operating_Airline, 
    OriginAirportID, 
    OriginAirportSeqID, 
    OriginCityMarketID, 
    OriginCityName, 
    OriginState, 
    OriginStateFips, 
    OriginStateName, 
    OriginWac, 
    DestAirportID, 
    DestAirportSeqID, 
    DestCityMarketID, 
    DestCityName, 
    DestState, 
    DestStateFips, 
    DestStateName, 
    DestWac, 
    DepDel15, 
    DepartureDelayGroups, 
    DepTimeBlk, 
    TaxiOut, 
    WheelsOff, 
    WheelsOn, 
    TaxiIn, 
    CRSArrTime, 
    ArrDelay, 
    ArrDel15, 
    ArrivalDelayGroups, 
    ArrTimeBlk, 
    DistanceGroup, 
    DivAirportLandings) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
    except Error as e:
        print(e)
    return cur.lastrowid

def insert_airport_record(conn, values):
    pass



