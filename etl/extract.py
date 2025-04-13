import pandas as pd
import os

# Base class for data extraction and cleaning
class Extractor:
    def __init__(self):
        pass

    # If the data can be extracted by this extractor, returns the extracted
    # datframe. Otherwise, return false
    def try_extract(self, path: str) -> pd.DataFrame | bool:
        raise NotImplementedError
