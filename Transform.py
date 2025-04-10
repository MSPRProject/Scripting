import pandas as pd
import numpy as np

def DataClean(define):
    define = define.dropna(subset=["Country/Region", "Confirmed", "Deaths", "Recovered", "Population"])
    define["Country/Region"] = define["Country/Region"].str.strip().str.title()
    return define

def DataTransform(define):
    define["Date"] = pd.to_datetime(define["Date"], errors='coerce')
    define["Confirmed"] = define["Confirmed"].astype(int)
    define["Deaths"] = define["Deaths"].astype(int)
    define["Country/Region"] = define["Country/Region"].str.title()
    return define

def RemoveDuplicate(define):
    define = define.drop_duplicates(subset=["Date", "Country/Region"], keep="last")
    return define

def DataAssembled(define):
    assembled = define.groupby("Country/Region").agg({
        "Confirmed": ["sum", "mean"],
        "Deaths": ["sum", "mean"],
        "Recovered": ["sum", "mean"],
        "Population": "mean"
    }).reset_index()

    assembled.columns = ["Country", "Total_Confirmed", "Average_Confirmed", 
                         "Total_Deaths", "Average_Deaths", "Total_Recovered", "Average_Recovered", "Population"]
    
    return assembled

def VerifiedIntegrate(define):
    errors = []

    if define["Confirmed"].lt(0).any():
        errors.append("Confirmed")
    if define["Deaths"].lt(0).any():
        errors.append("Deaths")
    if define["Deaths"].gt(define["Confirmed"]).any():
        errors.append("Deaths > Confirmed")

    if errors:
        print(f"Data validation errors: {', '.join(errors)}")
        for e in errors:
            print(" -", e)
        return False
    else:
        print("Data validation passed.")
        return True
    
def Transformer(define):
    define = DataClean(define)
    define = DataTransform(define)
    define = RemoveDuplicate(define)
    
    if not VerifiedIntegrate(define):
        raise ValueError("Data validation failed.")
    
    assembled = DataAssembled(define)
    return define, assembled
