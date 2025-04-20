from extract import Extractor
import pandas as pd

class MpoxExtractor(Extractor):
    def __init__(self):
        super().__init__()

    def try_extract(self, path: str) -> pd.DataFrame | bool:
        df = pd.read_csv(path)
        needed_columns = ["location", "iso_code", "date", "new_cases", "new_deaths", "total_cases", "total_deaths"]
        if any(col not in df.columns for col in needed_columns):
            return False

        df['date'] = pd.to_datetime(df['date'])

        # We are not interested in world-wide data
        df = df[df["location"] != "World"]

        # We need to map each location to a pair of continent/country
        df = df[df["location"].map(location_mapping).apply(lambda x: isinstance(x, dict)).astype(bool)]
        df[["continent", "country"]] = df["location"].map(location_mapping).apply(pd.Series)

        df["iso_code"] = df["iso_code"].replace("OWID_AFR", None).replace("OWID_ASI", None)
        df["pandemic"] = "mpox"

        df["population"] = None

        df = df.drop(["new_cases_smoothed", "new_deaths_smoothed", "new_cases_per_million", "location", "total_cases_per_million", "new_cases_smoothed_per_million", "new_deaths_per_million", "total_deaths_per_million", "new_deaths_smoothed_per_million"], axis="columns")
        return df

location_mapping = {
    "Africa": False,
    "Andorra": {"continent": "EUROPE", "country": "Andorra"},
    "Argentina": {"continent": "SOUTH_AMERICA", "country": "Argentina"},
    "Aruba": {"continent": "NORTH_AMERICA", "country": "Aruba"},
    "Asia": False,
    "Australia": {"continent": "OCEANIA", "country": "Australia"},
    "Austria": {"continent": "EUROPE", "country": "Austria"},
    "Bahamas": {"continent": "NORTH_AMERICA", "country": "Bahamas"},
    "Bahrain": {"continent": "ASIA", "country": "Bahrain"},
    "Barbados": {"continent": "NORTH_AMERICA", "country": "Barbados"},
    "Belgium": {"continent": "EUROPE", "country": "Belgium"},
    "Benin": {"continent": "AFRICA", "country": "Benin"},
    "Bermuda": {"continent": "NORTH_AMERICA", "country": "Bermuda"},
    "Bolivia": {"continent": "SOUTH_AMERICA", "country": "Bolivia"},
    "Bosnia and Herzegovina": {
        "continent": "EUROPE",
        "country": "Bosnia and Herzegovina",
    },
    "Brazil": {"continent": "SOUTH_AMERICA", "country": "Brazil"},
    "Bulgaria": {"continent": "EUROPE", "country": "Bulgaria"},
    "Cameroon": {"continent": "AFRICA", "country": "Cameroon"},
    "Canada": {"continent": "NORTH_AMERICA", "country": "Canada"},
    "Central African Republic": {
        "continent": "AFRICA",
        "country": "Central African Republic",
    },
    "Chile": {"continent": "SOUTH_AMERICA", "country": "Chile"},
    "China": {"continent": "ASIA", "country": "China"},
    "Colombia": {"continent": "SOUTH_AMERICA", "country": "Colombia"},
    "Congo": {"continent": "AFRICA", "country": "Congo"},
    "Costa Rica": {"continent": "NORTH_AMERICA", "country": "Costa Rica"},
    "Croatia": {"continent": "EUROPE", "country": "Croatia"},
    "Cuba": {"continent": "NORTH_AMERICA", "country": "Cuba"},
    "Curacao": {"continent": "NORTH_AMERICA", "country": "Curacao"},
    "Cyprus": {"continent": "EUROPE", "country": "Cyprus"},
    "Czechia": {"continent": "EUROPE", "country": "Czechia"},
    "Democratic Republic of Congo": {
        "continent": "AFRICA",
        "country": "Democratic Republic of Congo",
    },
    "Denmark": {"continent": "EUROPE", "country": "Denmark"},
    "Dominican Republic": {
        "continent": "NORTH_AMERICA",
        "country": "Dominican Republic",
    },
    "Ecuador": {"continent": "SOUTH_AMERICA", "country": "Ecuador"},
    "Egypt": {"continent": "AFRICA", "country": "Egypt"},
    "El Salvador": {"continent": "NORTH_AMERICA", "country": "El Salvador"},
    "Estonia": {"continent": "EUROPE", "country": "Estonia"},
    "Europe": False,
    "Finland": {"continent": "EUROPE", "country": "Finland"},
    "France": {"continent": "EUROPE", "country": "France"},
    "Georgia": {"continent": "ASIA", "country": "Georgia"},
    "Germany": {"continent": "EUROPE", "country": "Germany"},
    "Ghana": {"continent": "AFRICA", "country": "Ghana"},
    "Gibraltar": {"continent": "EUROPE", "country": "Gibraltar"},
    "Greece": {"continent": "EUROPE", "country": "Greece"},
    "Greenland": {"continent": "NORTH_AMERICA", "country": "Greenland"},
    "Guadeloupe": {"continent": "NORTH_AMERICA", "country": "Guadeloupe"},
    "Guam": {"continent": "OCEANIA", "country": "Guam"},
    "Guatemala": {"continent": "NORTH_AMERICA", "country": "Guatemala"},
    "Guyana": {"continent": "SOUTH_AMERICA", "country": "Guyana"},
    "Honduras": {"continent": "NORTH_AMERICA", "country": "Honduras"},
    "Hungary": {"continent": "EUROPE", "country": "Hungary"},
    "Iceland": {"continent": "EUROPE", "country": "Iceland"},
    "India": {"continent": "ASIA", "country": "India"},
    "Indonesia": {"continent": "ASIA", "country": "Indonesia"},
    "Iran": {"continent": "ASIA", "country": "Iran"},
    "Ireland": {"continent": "EUROPE", "country": "Ireland"},
    "Israel": {"continent": "ASIA", "country": "Israel"},
    "Italy": {"continent": "EUROPE", "country": "Italy"},
    "Jamaica": {"continent": "NORTH_AMERICA", "country": "Jamaica"},
    "Japan": {"continent": "ASIA", "country": "Japan"},
    "Jordan": {"continent": "ASIA", "country": "Jordan"},
    "Latvia": {"continent": "EUROPE", "country": "Latvia"},
    "Lebanon": {"continent": "ASIA", "country": "Lebanon"},
    "Liberia": {"continent": "AFRICA", "country": "Liberia"},
    "Lithuania": {"continent": "EUROPE", "country": "Lithuania"},
    "Luxembourg": {"continent": "EUROPE", "country": "Luxembourg"},
    "Malta": {"continent": "EUROPE", "country": "Malta"},
    "Martinique": {"continent": "NORTH_AMERICA", "country": "Martinique"},
    "Mexico": {"continent": "NORTH_AMERICA", "country": "Mexico"},
    "Moldova": {"continent": "EUROPE", "country": "Moldova"},
    "Monaco": {"continent": "EUROPE", "country": "Monaco"},
    "Montenegro": {"continent": "EUROPE", "country": "Montenegro"},
    "Morocco": {"continent": "AFRICA", "country": "Morocco"},
    "Mozambique": {"continent": "AFRICA", "country": "Mozambique"},
    "Netherlands": {"continent": "EUROPE", "country": "Netherlands"},
    "New Caledonia": {"continent": "OCEANIA", "country": "New Caledonia"},
    "New Zealand": {"continent": "OCEANIA", "country": "New Zealand"},
    "Nigeria": {"continent": "AFRICA", "country": "Nigeria"},
    "North America": False,
    "Norway": {"continent": "EUROPE", "country": "Norway"},
    "Oceania": False,
    "Pakistan": {"continent": "ASIA", "country": "Pakistan"},
    "Panama": {"continent": "NORTH_AMERICA", "country": "Panama"},
    "Paraguay": {"continent": "SOUTH_AMERICA", "country": "Paraguay"},
    "Peru": {"continent": "SOUTH_AMERICA", "country": "Peru"},
    "Philippines": {"continent": "ASIA", "country": "Philippines"},
    "Poland": {"continent": "EUROPE", "country": "Poland"},
    "Portugal": {"continent": "EUROPE", "country": "Portugal"},
    "Puerto Rico": {"continent": "NORTH_AMERICA", "country": "Puerto Rico"},
    "Qatar": {"continent": "ASIA", "country": "Qatar"},
    "Romania": {"continent": "EUROPE", "country": "Romania"},
    "Russia": {"continent": "EUROPE", "country": "Russia"},
    "Saint Martin (French part)": {
        "continent": "NORTH_AMERICA",
        "country": "Saint Martin (French part)",
    },
    "San Marino": {"continent": "EUROPE", "country": "San Marino"},
    "Saudi Arabia": {"continent": "ASIA", "country": "Saudi Arabia"},
    "Serbia": {"continent": "EUROPE", "country": "Serbia"},
    "Singapore": {"continent": "ASIA", "country": "Singapore"},
    "Slovakia": {"continent": "EUROPE", "country": "Slovakia"},
    "Slovenia": {"continent": "EUROPE", "country": "Slovenia"},
    "South Africa": {"continent": "AFRICA", "country": "South Africa"},
    "South America": False,
    "South Korea": {"continent": "ASIA", "country": "South Korea"},
    "Spain": {"continent": "EUROPE", "country": "Spain"},
    "Sri Lanka": {"continent": "ASIA", "country": "Sri Lanka"},
    "Sudan": {"continent": "AFRICA", "country": "Sudan"},
    "Sweden": {"continent": "EUROPE", "country": "Sweden"},
    "Switzerland": {"continent": "EUROPE", "country": "Switzerland"},
    "Thailand": {"continent": "ASIA", "country": "Thailand"},
    "Turkey": {"continent": "ASIA", "country": "Turkey"},
    "Ukraine": {"continent": "EUROPE", "country": "Ukraine"},
    "United Arab Emirates": {
        "continent": "ASIA",
        "country": "United Arab Emirates",
    },
    "United Kingdom": {"continent": "EUROPE", "country": "United Kingdom"},
    "United States": {"continent": "NORTH_AMERICA", "country": "United States"},
    "Uruguay": {"continent": "SOUTH_AMERICA", "country": "Uruguay"},
    "Venezuela": {"continent": "SOUTH_AMERICA", "country": "Venezuela"},
    "Vietnam": {"continent": "ASIA", "country": "Vietnam"},
    "World": False,
}
