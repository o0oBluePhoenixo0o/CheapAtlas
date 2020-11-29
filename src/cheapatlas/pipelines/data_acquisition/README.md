# Pipeline data_acquisition

> *Note:* This is a `README.md` boilerplate generated using `Kedro 0.16.6`.

## Overview

<!---
Please describe your modular pipeline here.
-->

The pipeline aims to gather data from 2 sources:
- Building objects per postal code data from OpenStreetMap through OverPass Turbo API
- OSM daily data dump for all Germany states from Geofabrik

## Pipeline inputs

<!---
The list of pipeline inputs.
-->

- List of all postal codes in Germany, obtain [HERE](!https://www.suche-postleitzahl.org/downloads)
- List of all states in Germany, edit in "parameters.yml"
## Pipeline outputs

<!---
The list of pipeline outputs.
-->

Obtained data is stored in "*data/01_raw*"
- Building objects data in their respective postal code folder (etc. "data/01_raw/buildings_01067.csv")
- OSM dump files in their respective state folder (etc. "data/01_raw/geofabrik/BW/<region_name>.pbf")