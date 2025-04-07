import psycopg2 
from psycopg2 import sql
import pandas as pd

def PostgresConnect(): 
    try:
        conn = psycopg2.connect(
            host="",
            database="",
            port=5432,
            user="",
            password=""
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise

def DataInsert(conn, define, table=""):
    cursor = conn.cursor()
    errors = []
    
    for index, row in define.iterrows():
        try : 
            query = sql.SQL("INSERT INTO {} (date, country, infected, deaths) VALUES (%s, %s, %s, %s)").format(sql.Identifier(table))
            
            cursor.execute(query, (row['date'], row['country'], row['infected'], row['deaths']))
            
        except Exception as e:
            errors.append((index, str(e)))
    
    conn.commit()
    cursor.close()
    
    if errors :
        for line, msg, in errors:
            print(f"Error inserting row {line}: {msg}")
        with open("error_log.txt", "w") as f:
            for line, msg in errors:
                f.write(f"Error inserting row {line}: {msg}\n")
    else :
        print("All rows inserted successfully.")