#!/usr/bin/env python

import pandas as pd
import numpy as np
import glob


STOP_DATA_OLD = pd.read_csv("../data/raw/Stop_Data.csv.gz", low_memory=False)
STOP_DATA_2023_2024 = pd.read_csv(
    "../data/raw/Stop_Data_2023-2024.csv.gz", low_memory=False
)

ARREST_DATA = pd.read_csv("../data/raw/Adult_Arrests.csv.gz", low_memory=False)
ARREST_DATA_2023 = pd.read_csv("../data/raw/Adult_Arrests_2023.csv", low_memory=False)

INCIDENT_DATA = pd.concat(
    map(pd.read_csv, glob.glob("../data/raw/Crime_Incidents*")), ignore_index=True
)
INCIDENT_DATA_ALL = pd.read_csv(
    "../data/raw/dc-crimes-search-results.csv", low_memory=False
)

COLUMN_TRANSLATION_2023 = {
    "Arrestee Type": "TYPE",
    "Arrest Year": "YEAR",
    "Arrest Date": "DATE_",
    "Arrest Hour": "HOUR",
    "CCN": "CCN",
    "Arrest Number#": "ARREST_NUMBER",
    "Age": "AGE",
    "Defendant PSA": "DEFENDANT_PSA",
    "Defendant District": "DEFENDANT_DISTRICT",
    "Defendant Race": "RACE",
    "Defendant Ethnicity": "ETHNICITY",
    "Defendant Sex": "SEX",
    "Arrest Category": "CATEGORY",
    "Charge Description": "DESCRIPTION",
    "Arrest Location PSA": "ARREST_PSA",
    "Arrest Location District": "ARREST_DISTRICT",
    "Arrest Block GEOX": "ARREST_BLOCKX",
    "Arrest Block GEOY": "ARREST_BLOCKY",
    "Arrest Latitude": "ARREST_LATITUDE",
    "Arrest Longitude": "ARREST_LONGITUDE",
    "Offense Location PSA": "OFFENSE_PSA",
    "Offense Location District": "OFFENSE_DISTRICT",
    "Offense Block GEOX": "OFFENSE_BLOCKX",
    "Offense Block GEOY": "OFFENSE_BLOCKY",
    "Offense Latitude": "OFFENSE_LATITUDE",
    "Offense Longitude": "OFFENSE_LONGITUDE",
}


def data_cleanup(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    Parse dates and create some convenience fields
    """
    df["date"] = pd.to_datetime(df[date_col])
    df["month_year"] = df.date.dt.strftime("%Y-%m")
    if "YEAR" not in df.columns:
        df["year"] = df.date.dt.strftime("%Y")
    df.columns = [c.lower() for c in df.columns]
    return df


def arrest_category_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Data through 2017 has "Na" dropped from the strings in the category field,
    replaced with a single space.
    Like Dave says, if you don't have the right tools, don't use a spoon.
    This also corrects an unrelated errors in the Release Violations and
    Fraud/Financial categories, rolling up a few random values.
    """
    fixes = {
        " rcotics": "Narcotics",
        "Fraud and Fi ncial Crimes": "Fraud and Financial Crimes",
        "Fraud and Financial Crimes (Coun)": "Fraud and Financial Crimes",
        "Fraud and Financial Crimes (Forg)": "Fraud and Financial Crimes",
        "Fraud and Financial Crimes (Frau)": "Fraud and Financial Crimes",
        "Kid pping": "Kidnapping",
        "Release Violations/Fugitive (Fug)": "Release Violations/Fugitive",
        "Release Violations/Fugitive (Warr)": "Release Violations/Fugitive",
        "Release Violations": "Release Violations/Fugitive",
    }

    df["category"] = df.category.apply(lambda x: fixes.get(x, x))
    return df


if __name__ == "__main__":
    STOP_DATA_COLS = list(set(STOP_DATA_OLD.columns) & set(STOP_DATA_2023_2024.columns))
    STOP_DATA = pd.concat(
        [STOP_DATA_OLD[STOP_DATA_COLS], STOP_DATA_2023_2024[STOP_DATA_COLS]]
    )

    data_cleanup(STOP_DATA, "DATETIME").drop_duplicates(subset="ccn_anonymized").to_csv(
        "../data/clean/stop_data.csv.gz", index=False, compression="gzip"
    )

    ARREST_DATA_2023.columns = [
        COLUMN_TRANSLATION_2023[c] for c in ARREST_DATA_2023.columns
    ]

    ARREST_DATA_PRE_23 = data_cleanup(ARREST_DATA, "DATE_")

    ARREST_DATA_2023 = data_cleanup(ARREST_DATA_2023, "DATE_")

    ARREST_DATA = pd.concat(
        [ARREST_DATA_PRE_23, ARREST_DATA_2023], ignore_index=True
    ).to_csv("../data/clean/arrest_data.csv.gz", index=False, compression="gzip")

    THREE11_FILES = glob.glob("../data/raw/311_City_Service_Requests*.csv.gz")

    THREE11_YEARS = []

    # Loop through each file and append to the combined dataframe
    for file in THREE11_FILES:
        df = pd.read_csv(file)
        THREE11_YEARS.append(df)

    THREE11_DATA = pd.concat(THREE11_YEARS, ignore_index=True)

    for idx, chunk in enumerate(np.array_split(THREE11_DATA, 3)):
        chunk.to_csv(
            f"../data/clean/311_data_part_{idx}.csv.gz", index=False, compression="gzip"
        )

    data_cleanup(INCIDENT_DATA, "START_DATE").to_csv(
        "../data/clean/incident_data.csv.gz", index=False, compression="gzip"
    )

    data_cleanup(INCIDENT_DATA_ALL, "START_DATE").to_csv(
        "../data/clean/incident_data_all.csv.gz", index=False, compression="gzip"
    )
