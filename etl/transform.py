import pandas as pd
import numpy as np

class Transformer:
    def __init__(self):
        pass

    def transform(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        country_df = df[['continent', 'country', 'iso_code', 'population']].drop_duplicates().rename(
            columns={
                'continent': 'continent',
                'country': 'name',
                'iso_code': 'iso3'
            }
        )

        infection_df = (
            df.groupby(['country', 'pandemic'], as_index=False)
            .agg({
                'total_cases': 'max',
                'total_deaths': 'max'
            })
            .rename(columns={
                'total_cases': 'total_cases',
                'total_deaths': 'total_deaths'
            })
        )
        infection_df = infection_df[['country', 'pandemic', 'total_cases', 'total_deaths']]

        report_df = df[['country', 'pandemic', 'date', 'new_cases', 'new_deaths']].rename(
            columns={
                'date': 'date',
                'new_cases': 'new_cases',
                'new_deaths': 'new_deaths'
            }
        )

        # Sanity checks
        for country in country_df['name'].unique():
            assert country_df[country_df['name'] == country].shape[0] == 1

        # Return all the transformed dataframes
        return country_df, infection_df, report_df
