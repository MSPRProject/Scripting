import pandas as pd
from pandas import Timestamp
from tqdm import tqdm
import requests

# TODO: Use API

class Loader:
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url
        self.country_cache = {}
        self.pandemic_cache = {}
        self.infection_cache = {}
        self.report_cache = {}

    def _load_country(self, country: pd.DataFrame):
        iso3 = country['iso3']
        name = country['name']
        continent = country['continent']
        population = country['population']

        if name in self.country_cache:
            return

        # Check if the country already exists in the database
        response = requests.get(
            f"{self.api_base_url}/countries/search/findByName",
            params={"name": name}
        )

        if response.status_code == 200:
            country_data = response.json()
        elif response.status_code == 404:
            # Country does not exist, create it
            payload = {
                "name": name,
                "iso3": iso3,
                "continent": continent,
                "population": population
            }
            create_response = requests.post(
                f"{self.api_base_url}/countries",
                json=payload
            )
            if create_response.status_code == 201:
                country_data = create_response.json()
            else:
                raise Exception(f"Failed to create country {name}: {create_response.text}")
        else:
            raise Exception(f"Error checking country {name}: {response.text}")

        self.country_cache[name] = country_data['_links']['self']['href']

    def _load_pandemic(self, pandemic: str):
        if pandemic in self.pandemic_cache:
            return

        name = pandemic
        pathogen = None
        description = None
        start_date = None
        end_date = None
        notes = None

        # Check if the pandemic already exists in the database
        response = requests.get(
            f"{self.api_base_url}/pandemics/search/findByName",
            params={"name": name}
        )

        if response.status_code == 200:
            pandemic_data = response.json()
        elif response.status_code == 404:
            # Pandemic does not exist, create it
            payload = {
                "name": name,
                "pathogen": pathogen,
                "description": description,
                "start_date": start_date,
                "end_date": end_date,
                "notes": notes
            }
            create_response = requests.post(
                f"{self.api_base_url}/pandemics",
                json=payload
            )
            if create_response.status_code == 201:
                pandemic_data = create_response.json()
            else:
                raise Exception(f"Failed to create pandemic {name}: {create_response.text}")
        else:
            raise Exception(f"Error checking pandemic {name}: {response.text}")


        self.pandemic_cache[name] = pandemic_data['_links']['self']['href']

    def _load_infection(self, infection: pd.DataFrame):
        country = self.country_cache[infection['country']]
        pandemic = self.pandemic_cache[infection['pandemic']]
        total_cases = infection['total_cases']
        total_deaths = infection['total_deaths']

        if (infection['country'], infection['pandemic']) in self.infection_cache:
            return

        # Check if the infection already exists in the database
        response = requests.get(
            f"{self.api_base_url}/infections/search/findByPandemicAndCountry",
            params={'pandemic_id': int(pandemic.split('/')[-1]), 'country_id': int(country.split('/')[-1])}
        )

        if response.status_code == 200:
            infection_data = response.json()
        elif response.status_code == 404:
            # Infection does not exist, create it
            payload = {
                "country": country,
                "pandemic": pandemic,
                "total_cases": total_cases,
                "total_deaths": total_deaths
            }
            create_response = requests.post(
                f"{self.api_base_url}/infections",
                json=payload
            )
            if create_response.status_code == 201:
                infection_data = create_response.json()
            else:
                raise Exception(f"Failed to create infection: {create_response.text}")
        else:
            raise Exception(f"Error checking infection: {response.text}")

        self.infection_cache[(infection['country'], infection['pandemic'])] = infection_data["_links"]["self"]["href"]

    def _load_report(self, report: pd.DataFrame):
        infection = self.infection_cache[(report['country'], report['pandemic'])]
        date: Timestamp = report['date']
        new_cases = report['new_cases']
        new_deaths = report['new_deaths']

        if (infection, date) in self.report_cache:
            print(f"WARNING: Report already exists for {infection} on {date}")

        # Check if the report already exists in the database
        response = requests.get(
            f"{self.api_base_url}/reports/search/findByDateAndCountryIdAndPandemicId",
            params={
                "date": date.date().isoformat(),
                "country_id": int(self.country_cache[report['country']].split("/")[-1]),
                "pandemic_id": int(self.pandemic_cache[report['pandemic']].split("/")[-1])
            }
        )

        if response.status_code == 200:
            print(f"WARNING: Report already exists for {infection} on {date}")
        elif response.status_code == 404:
            # Report does not exist, create it
            payload = {
                "infection": infection,
                "date": date.date().isoformat(),
                "new_cases": new_cases,
                "new_deaths": new_deaths
            }
            create_response = requests.post(
                f"{self.api_base_url}/reports",
                json=payload
            )
            if create_response.status_code != 201:
                raise Exception(f"Failed to create report: {create_response.text}")
        else:
            raise Exception(f"Error checking report: {response.text}")

    def load(self, country_df: pd.DataFrame, infection_df: pd.DataFrame, report_df: pd.DataFrame):
        print(f"Loading {country_df.shape[0]} countries, {infection_df.shape[0]} infections, {report_df.shape[0]} reports")
        # Load countries
        for _, country in tqdm(country_df.iterrows(), desc="Loading countries", total=country_df.shape[0]):
            self._load_country(country)

        # Load pandemics
        for pandemic in tqdm(infection_df["pandemic"].unique(), desc="Loading pandemics", total=len(infection_df["pandemic"].unique())):
            self._load_pandemic(pandemic)

        # Load infections
        for _, infection in tqdm(infection_df.iterrows(), desc="Loading infections", total=infection_df.shape[0]):
            self._load_infection(infection)

        # Load reports
        for _, report in tqdm(report_df.iterrows(), desc="Loading reports", total=report_df.shape[0]):
            self._load_report(report)
