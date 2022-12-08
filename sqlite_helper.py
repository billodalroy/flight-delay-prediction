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
    sql = '''INSERT INTO raw_data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
    except Error as e:
        print(e)
    return cur.lastrowid

def insert_airport(conn, values):
    sql = '''INSERT INTO Airports VALUES(?,?,?)'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
    except Error as e:
        print(e)
    return cur.lastrowid


def insert_route(conn, values):
    sql = '''INSERT INTO routes (Source, Destination) VALUES(?,?)'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
    except Error as e:
        print(e)
    return cur.lastrowid
    
def insert_flight(conn, values):
    sql = '''INSERT INTO flights (FlightDate, RouteID, Cancelled, Diverted, CRSDepTime, 
    DepTime, DepDelayMinutes, DepDelay, ArrTime, ArrDelayMinutes, AirTime, Distance, IATA_Code_Marketing_Airline,
    Flight_Number_Operating_Airline) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
    except Error as e:
        print(e) 
    return cur.lastrowid


