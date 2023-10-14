#!/usr/bin/env python

import pandas as pd
import glob


STOP_DATA_OLD = pd.read_csv("../data/raw/Stop_Data.csv.gz", low_memory=False)
STOP_DATA_2023 = pd.read_csv("../data/raw/Stop_Data_2023.csv.gz", low_memory=False)

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


if __name__ == "__main__":
    STOP_DATA_COLS = STOP_DATA_OLD.columns & STOP_DATA_2023.columns
    STOP_DATA = pd.concat(
        [STOP_DATA_OLD[STOP_DATA_COLS], STOP_DATA_2023[STOP_DATA_COLS]]
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

    data_cleanup(INCIDENT_DATA_ALL, "START_DATE").to_csv(
        "../data/clean/incident_data_all.csv.gz", index=False, compression="gzip"
    )
