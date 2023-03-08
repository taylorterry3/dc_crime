#!/usr/bin/env python

import pandas as pd
import glob


def data_cleanup(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    df["month_year"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m")
    df["year"] = pd.to_datetime(df[date_col]).dt.strftime("%Y")
    df.columns = [c.lower() for c in df.columns]
    return df


STOP_DATA = pd.read_csv("../data/raw/Stop_Data.csv.gz")
ARREST_DATA = pd.read_csv("../data/raw/Adult_Arrests.csv.gz")
INCIDENT_DATA = pd.concat(
    map(pd.read_csv, glob.glob("../data/raw/Crime_Incidents*")), ignore_index=True
)


data_cleanup(STOP_DATA, "DATETIME").to_csv(
    "../data/clean/stop_data.csv.gz", index=False, compression="gzip"
)
data_cleanup(ARREST_DATA, "DATE_").to_csv(
    "../data/clean/arrest_data.csv.gz", index=False, compression="gzip"
)
data_cleanup(INCIDENT_DATA, "START_DATE").to_csv(
    "../data/clean/incident_data.csv.gz", index=False, compression="gzip"
)
