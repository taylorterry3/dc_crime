Data Sources
---

## Crime Data
- Adult_Arrests.csv: https://opendata.dc.gov/datasets/adult-arrests/explore
- Annual files get posted here: https://mpdc.dc.gov/node/1379551. The open data portal takes a while to get updated so I've temporarily merged the 2023 data onto the master file. The layouts are the same but the quoting is different; this is handled in ETL.py
- Crime_Incidents_in_20**.csv: https://opendata.dc.gov/search?collection=Dataset&q=crime%20incidents&sort=-created. 2023 data is only through Sept 10
- dc-crimes-search-results.csv: https://crimecards.dc.gov/all:crimes/all:weapons/8:years/citywide:ward. More up to date than annual incident data, goes back 8 years.
- Juvenile_Arrests.csv: https://opendata.dc.gov/datasets/juvenile-arrests/explore
- officers.csv: https://mpdc.dc.gov/page/monthly-staffing-reports transcribed from graphic.
- Stop_Data.csv: https://opendata.dc.gov/datasets/stop-data/explore. This is a mess, they started breaking it up into different year groups. More info here: https://mpdc.dc.gov/stopdata
- Stop_Data_2023-2024.csv.gz. This data was originally just the 2023 data set. On October 11th 2024 they released the 2024H1 data, but it has a ton of duplicate rows. Fortunately the new data can be deduped on the anonymized CCN field.


Some files gzipped to reduce size.

## ANC & 311 data
TODO
