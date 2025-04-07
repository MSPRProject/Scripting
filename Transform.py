import pandas as pd
import numpy as np

def DataClean(define) :
    define = define.dropna(subset=[])
    define["country"] = define["country"].str.strip().str.title()
    return define 

def DataTransform(define) :
    define["date"] = pd.to_datetime(define["date"], errors='coerce')
    define["infected"] = define["infected"].astype(int)
    define["deaths"] = define["deaths"].astype(int)
    define["country"] = define["country"].str.title()
    return define

def RemoveDuplicate(define) : 
    define = define.duplicates(subset=["date", "country"], keep="last")
    return define

def DataAssembled(define) :
    assembled = define.groupby("country").agg({
        "infected": ["sum", "mean"],
        "deaths": ["sum", "mean"],
    }).reset_index()
    assembled.columns = ["country", "infected", "deaths"]
    return assembled

def VerifiedIntegrate(define) : 
    errors = []
    
    if define["infected"].lt(0).any() :
        errors.append("infected")
    if define["deaths"].lt(0).any() :
        errors.append("deaths")
    if define["deaths"].gt(define["infected"]).any() :
        errors.append("deaths > infected")
        
    if errors :
        print(f"Data validation errors: {', '.join(errors)}")
        for e in errors :
            print(" -", e)
        return False
    else :
        print("Data validation passed.")
        return True
    
def Transformer(define) :
    define = DataClean(define)
    define = DataTransform(define)
    define = RemoveDuplicate(define)
    
    if not VerifiedIntegrate(define) :
        raise ValueError("Data validation failed.")
    
    assembled = DataAssembled
    return define, assembled