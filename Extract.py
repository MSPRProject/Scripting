import pandas as pd
import json
import os

EXPECTED_COLUMNS = {
    "date": "datetime64[ns]",
    "country": "object",
    "infected": "int64",
    "dead": "int64"
}

def FileType(file) :
    if file.endswitch(".csv"):
        return "csv"
    elif file.endswitch(".json"):
        return "json"
    else :
        raise ValueError("Unsupported file type. Only .csv and .json files are supported.")
    
def ReadFile(file) :
    file_type = FileType(file)
    try : 
        if file_type == ".csv" :
            df = pd.read_csv(file)
        elif file_type == ".json" :
            with open(file, 'r') as f:
                data = json.load(f)
            df = pd.json_normalize(data)
            
        print(f"File {file} read successfully.")
        
        return df
    
    except Exception as e:
        print(f"An error occurred while reading the file {file}: {e}")
        raise
    
def Validate(define) :
    if define is None :
        return False
    
    fail_columns = [column for column in EXPECTED_COLUMNS if column not in define.columns]
    if fail_columns :
        print(f"Missing columns: {', '.join(fail_columns)}")
        return False
    
    try : 
        for column, expected in EXPECTED_COLUMNS.items() :
            if "datetime" in expected: 
                define[column] = pd.to_datetime(define[column], errors='coerce')
            elif "int" in expected:
                define[column] = pd.to_numeric(define[column], errors='coerce')
            elif expected == "object":
                define[column] = define[column].astype(str)
                
            if define[column].isnull().any():
                print(f"Column {column} contains null values after conversion.")
                return False
            
        print(f"All columns validated successfully.")
        
        return True
    except Exception as e:
        print(f"An error occurred during validation: {e}")
        
        return False
    