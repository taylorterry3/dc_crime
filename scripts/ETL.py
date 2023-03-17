#!/usr/bin/env python

import pandas as pd
import glob


STOP_DATA = pd.read_csv("../data/raw/Stop_Data.csv.gz", low_memory=False)
ARREST_DATA = pd.read_csv("../data/raw/Adult_Arrests.csv.gz", low_memory=False)
INCIDENT_DATA = pd.concat(
    map(pd.read_csv, glob.glob("../data/raw/Crime_Incidents*")), ignore_index=True
)
INCIDENT_DATA_ALL = pd.read_csv(
    "../data/raw/dc-crimes-search-results.csv", low_memory=False
)


def data_cleanup(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    Parse dates and create some convenience fields
    """
    df["date"] = pd.to_datetime(df[date_col])
    df["month_year"] = df.date.dt.strftime("%Y-%m")
    df["year"] = df.date.dt.strftime("%Y")
    df.columns = [c.lower() for c in df.columns]
    return df


data_cleanup(STOP_DATA, "DATETIME").to_csv(
    "../data/clean/stop_data.csv.gz", index=False, compression="gzip"
)

data_cleanup(ARREST_DATA, "DATE_").to_csv(
    "../data/clean/arrest_data.csv.gz", index=False, compression="gzip"
)

data_cleanup(INCIDENT_DATA, "START_DATE").to_csv(
    "../data/clean/incident_data.csv.gz", index=False, compression="gzip"
)

data_cleanup(INCIDENT_DATA_ALL, "START_DATE").to_csv(
    "../data/clean/incident_data_all.csv.gz", index=False, compression="gzip"
)

FBI_PROPERTY = pd.read_csv("../data/raw/FBI_Property_Crime.csv").T.iloc[1:, 1]
FBI_VIOLENT = pd.read_csv("../data/raw/FBI_Violent_Crime.csv").T.iloc[1:, 1]

FBI_ALL = pd.concat([FBI_PROPERTY, FBI_VIOLENT], axis=1).reset_index()
FBI_ALL.columns = [
    "Year",
    "Property Crime Rate per 100k",
    "Violent Crime Rate per 100k",
]

FBI_ALL.to_csv("../data/clean/FBI_Data.csv", index=False)
