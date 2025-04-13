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

        # We are not interested in world-wide data
        df = df.drop(df[df["location"] == "World"].index)

        # We need to map each location to a pair of continent/country
        df = df[df["location"].map(location_mapping).astype(bool)]
        df[["continent", "country"]] = df["location"].map(location_mapping).apply(pd.Series)

        df["iso_code"] = df["iso_code"].replace("OWID_AFR", None).replace("OWID_ASI", None)
        df["pandemic"] = "mpox"

        df = df.drop(["new_cases_smoothed", "new_deaths_smoothed", "new_cases_per_million", "location", "total_cases_per_million", "new_cases_smoothed_per_million", "new_deaths_per_million", "total_deaths_per_million", "new_deaths_smoothed_per_million"], axis="columns")
        return df

location_mapping = {
    "Africa": False,
    "Andorra": {"continent": "Europe", "country": "Andorra"},
    "Argentina": {"continent": "South America", "country": "Argentina"},
    "Aruba": {"continent": "North America", "country": "Aruba"},
    "Asia": False,
    "Australia": {"continent": "Oceania", "country": "Australia"},
    "Austria": {"continent": "Europe", "country": "Austria"},
    "Bahamas": {"continent": "North America", "country": "Bahamas"},
    "Bahrain": {"continent": "Asia", "country": "Bahrain"},
    "Barbados": {"continent": "North America", "country": "Barbados"},
    "Belgium": {"continent": "Europe", "country": "Belgium"},
    "Benin": {"continent": "Africa", "country": "Benin"},
    "Bermuda": {"continent": "North America", "country": "Bermuda"},
    "Bolivia": {"continent": "South America", "country": "Bolivia"},
    "Bosnia and Herzegovina": {
        "continent": "Europe",
        "country": "Bosnia and Herzegovina",
    },
    "Brazil": {"continent": "South America", "country": "Brazil"},
    "Bulgaria": {"continent": "Europe", "country": "Bulgaria"},
    "Cameroon": {"continent": "Africa", "country": "Cameroon"},
    "Canada": {"continent": "North America", "country": "Canada"},
    "Central African Republic": {
        "continent": "Africa",
        "country": "Central African Republic",
    },
    "Chile": {"continent": "South America", "country": "Chile"},
    "China": {"continent": "Asia", "country": "China"},
    "Colombia": {"continent": "South America", "country": "Colombia"},
    "Congo": {"continent": "Africa", "country": "Congo"},
    "Costa Rica": {"continent": "North America", "country": "Costa Rica"},
    "Croatia": {"continent": "Europe", "country": "Croatia"},
    "Cuba": {"continent": "North America", "country": "Cuba"},
    "Curacao": {"continent": "North America", "country": "Curacao"},
    "Cyprus": {"continent": "Europe", "country": "Cyprus"},
    "Czechia": {"continent": "Europe", "country": "Czechia"},
    "Democratic Republic of Congo": {
        "continent": "Africa",
        "country": "Democratic Republic of Congo",
    },
    "Denmark": {"continent": "Europe", "country": "Denmark"},
    "Dominican Republic": {
        "continent": "North America",
        "country": "Dominican Republic",
    },
    "Ecuador": {"continent": "South America", "country": "Ecuador"},
    "Egypt": {"continent": "Africa", "country": "Egypt"},
    "El Salvador": {"continent": "North America", "country": "El Salvador"},
    "Estonia": {"continent": "Europe", "country": "Estonia"},
    "Europe": False,
    "Finland": {"continent": "Europe", "country": "Finland"},
    "France": {"continent": "Europe", "country": "France"},
    "Georgia": {"continent": "Asia", "country": "Georgia"},
    "Germany": {"continent": "Europe", "country": "Germany"},
    "Ghana": {"continent": "Africa", "country": "Ghana"},
    "Gibraltar": {"continent": "Europe", "country": "Gibraltar"},
    "Greece": {"continent": "Europe", "country": "Greece"},
    "Greenland": {"continent": "North America", "country": "Greenland"},
    "Guadeloupe": {"continent": "North America", "country": "Guadeloupe"},
    "Guam": {"continent": "Oceania", "country": "Guam"},
    "Guatemala": {"continent": "North America", "country": "Guatemala"},
    "Guyana": {"continent": "South America", "country": "Guyana"},
    "Honduras": {"continent": "North America", "country": "Honduras"},
    "Hungary": {"continent": "Europe", "country": "Hungary"},
    "Iceland": {"continent": "Europe", "country": "Iceland"},
    "India": {"continent": "Asia", "country": "India"},
    "Indonesia": {"continent": "Asia", "country": "Indonesia"},
    "Iran": {"continent": "Asia", "country": "Iran"},
    "Ireland": {"continent": "Europe", "country": "Ireland"},
    "Israel": {"continent": "Asia", "country": "Israel"},
    "Italy": {"continent": "Europe", "country": "Italy"},
    "Jamaica": {"continent": "North America", "country": "Jamaica"},
    "Japan": {"continent": "Asia", "country": "Japan"},
    "Jordan": {"continent": "Asia", "country": "Jordan"},
    "Latvia": {"continent": "Europe", "country": "Latvia"},
    "Lebanon": {"continent": "Asia", "country": "Lebanon"},
    "Liberia": {"continent": "Africa", "country": "Liberia"},
    "Lithuania": {"continent": "Europe", "country": "Lithuania"},
    "Luxembourg": {"continent": "Europe", "country": "Luxembourg"},
    "Malta": {"continent": "Europe", "country": "Malta"},
    "Martinique": {"continent": "North America", "country": "Martinique"},
    "Mexico": {"continent": "North America", "country": "Mexico"},
    "Moldova": {"continent": "Europe", "country": "Moldova"},
    "Monaco": {"continent": "Europe", "country": "Monaco"},
    "Montenegro": {"continent": "Europe", "country": "Montenegro"},
    "Morocco": {"continent": "Africa", "country": "Morocco"},
    "Mozambique": {"continent": "Africa", "country": "Mozambique"},
    "Netherlands": {"continent": "Europe", "country": "Netherlands"},
    "New Caledonia": {"continent": "Oceania", "country": "New Caledonia"},
    "New Zealand": {"continent": "Oceania", "country": "New Zealand"},
    "Nigeria": {"continent": "Africa", "country": "Nigeria"},
    "North America": False,
    "Norway": {"continent": "Europe", "country": "Norway"},
    "Oceania": False,
    "Pakistan": {"continent": "Asia", "country": "Pakistan"},
    "Panama": {"continent": "North America", "country": "Panama"},
    "Paraguay": {"continent": "South America", "country": "Paraguay"},
    "Peru": {"continent": "South America", "country": "Peru"},
    "Philippines": {"continent": "Asia", "country": "Philippines"},
    "Poland": {"continent": "Europe", "country": "Poland"},
    "Portugal": {"continent": "Europe", "country": "Portugal"},
    "Puerto Rico": {"continent": "North America", "country": "Puerto Rico"},
    "Qatar": {"continent": "Asia", "country": "Qatar"},
    "Romania": {"continent": "Europe", "country": "Romania"},
    "Russia": {"continent": "Europe", "country": "Russia"},
    "Saint Martin (French part)": {
        "continent": "North America",
        "country": "Saint Martin (French part)",
    },
    "San Marino": {"continent": "Europe", "country": "San Marino"},
    "Saudi Arabia": {"continent": "Asia", "country": "Saudi Arabia"},
    "Serbia": {"continent": "Europe", "country": "Serbia"},
    "Singapore": {"continent": "Asia", "country": "Singapore"},
    "Slovakia": {"continent": "Europe", "country": "Slovakia"},
    "Slovenia": {"continent": "Europe", "country": "Slovenia"},
    "South Africa": {"continent": "Africa", "country": "South Africa"},
    "South America": False,
    "South Korea": {"continent": "Asia", "country": "South Korea"},
    "Spain": {"continent": "Europe", "country": "Spain"},
    "Sri Lanka": {"continent": "Asia", "country": "Sri Lanka"},
    "Sudan": {"continent": "Africa", "country": "Sudan"},
    "Sweden": {"continent": "Europe", "country": "Sweden"},
    "Switzerland": {"continent": "Europe", "country": "Switzerland"},
    "Thailand": {"continent": "Asia", "country": "Thailand"},
    "Turkey": {"continent": "Asia", "country": "Turkey"},
    "Ukraine": {"continent": "Europe", "country": "Ukraine"},
    "United Arab Emirates": {
        "continent": "Asia",
        "country": "United Arab Emirates",
    },
    "United Kingdom": {"continent": "Europe", "country": "United Kingdom"},
    "United States": {"continent": "North America", "country": "United States"},
    "Uruguay": {"continent": "South America", "country": "Uruguay"},
    "Venezuela": {"continent": "South America", "country": "Venezuela"},
    "Vietnam": {"continent": "Asia", "country": "Vietnam"},
    "World": False,
}
