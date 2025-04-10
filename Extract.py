import pandas as pd
import json
import os

EXPECTED_COLUMNS = {
    "Date": "datetime64[ns]",
    "Country/Region": "object",
    "Confirmed": "int64",
    "Deaths": "int64",
    "Recovered": "int64",
    "Active": "int64",
    "New cases": "int64",
    "New deaths": "int64",
    "New recovered": "int64",
    "Deaths / 100 Cases": "float64",
    "Recovered / 100 Cases": "float64",
    "Deaths / 100 Recovered": "float64",
    "Confirmed last week": "int64",
    "1 week change": "int64",
    "1 week % increase": "float64",
    "WHO Region": "object",
    "Province/State": "object",
    "Population": "int64",
    "TotalCases": "int64",
    "NewCases": "int64",
    "TotalDeaths": "int64",
    "NewDeaths": "int64",
    "TotalRecovered": "int64",
    "NewRecovered": "int64",
    "ActiveCases": "int64",
    "Serious,Critical": "int64",
    "Tot Cases/1M pop": "float64",
    "Deaths/1M pop": "float64",
    "TotalTests": "int64",
    "Tests/1M pop": "float64"
}

def FileType(file):
    if file.endswith(".csv"):
        return "csv"
    elif file.endswith(".json"):
        return "json"
    else:
        raise ValueError("Unsupported file type. Only .csv and .json files are supported.")
    
def ReadFile(file):
    df = None
    file_type = FileType(file)
    try:
        if file_type == "csv":
            df = pd.read_csv(file)
        elif file_type == "json":
            with open(file, 'r') as f:
                data = json.load(f)
            df = pd.json_normalize(data)
            
        print(f"File {file} read successfully.")
        return df
    
    except Exception as e:
        print(f"An error occurred while reading the file {file}: {e}")
        raise
    
    if df is None:
        print(f"Failed to read the file {file}.")
        return None

df = ReadFile("data_mspr.csv")

def Validate(df):
    if df is None:
        return False

    fail_columns = [column for column in EXPECTED_COLUMNS if column not in df.columns]
    if fail_columns:
        print(f"Missing columns: {', '.join(fail_columns)}")
        return False

    # Clean missing values
    df = df.dropna(subset=EXPECTED_COLUMNS.keys(), how='any') 
    df.fillna(0, inplace=True) 

    try:
        for column, expected in EXPECTED_COLUMNS.items():
            if "datetime" in expected:
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif "int" in expected:
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif expected == "object":
                df[column] = df[column].astype(str)

            if df[column].isnull().any():
                print(f"Column {column} contains null values after conversion.")
                return False

        print("All columns validated successfully.")
        return True
    except Exception as e:
        print(f"An error occurred during validation: {e}")
        return False

if not Validate(df):
    raise ValueError("Data validation failed.")
