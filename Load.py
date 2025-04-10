import psycopg2 
from psycopg2 import sql
import pandas as pd

def PostgresConnect(): 
    try:
        conn = psycopg2.connect(
            host="localhost", 
            database="data_mspr",
            port=5432,
            user="username",
            password="password"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise

def DataInsert(conn, define, table="data_mspr"):
    cursor = conn.cursor()
    errors = []
    
    for index, row in define.iterrows():
        try:
            query = sql.SQL("""
                INSERT INTO {table} (Date, Country, Confirmed, Deaths, Recovered, Active, 
                                     New_cases, New_deaths, New_recovered, Deaths_per_100_cases, 
                                     Recovered_per_100_cases, Deaths_per_100_recovered, 
                                     Confirmed_last_week, Week_change, Week_percent_increase, 
                                     WHO_Region, Province_State, Lat, Long, Population)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """).format(sql.Identifier(table))
            
            cursor.execute(query, (
                row['Date'], row['Country'], row['Confirmed'], row['Deaths'], row['Recovered'], 
                row['Active'], row['New cases'], row['New deaths'], row['New recovered'], 
                row['Deaths / 100 Cases'], row['Recovered / 100 Cases'], row['Deaths / 100 Recovered'], 
                row['Confirmed last week'], row['1 week change'], row['1 week % increase'], 
                row['WHO Region'], row['Province/State'], row['Lat'], row['Long'], row['Population']
            ))

        except Exception as e:
            errors.append((index, str(e)))

    conn.commit()
    cursor.close()

    if errors:
        for line, msg in errors:
            print(f"Error inserting row {line}: {msg}")
        with open("error_log.txt", "w") as f:
            for line, msg in errors:
                f.write(f"Error inserting row {line}: {msg}\n")
    else:
        print("All rows inserted successfully.")
