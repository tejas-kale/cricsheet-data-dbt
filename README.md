## Introduction

In this project, I will use dbt to model and maintain cricket data for exploration and visualisation. Briefly, the following actions are performed:

1. Data is ingested to Google BigQuery from CSV files using a Python script.

## Data

[Cricsheet](https://cricsheet.org) is a website that provides ball-by-ball structured data for almost every cricket match played around the world since 2004. The data is available in different formats and we use the “Ashwin” CSV format which splits the data for each match into two files - an info file that contains match information and a ball-by-ball data file. Further documentation about the data schema can be read [here](https://cricsheet.org/format/csv_ashwin/#the-match-info-file).

As of January 30, 2024, data for 16064 matches is available with match information missing for the following 5 match IDs:

- `1156654`
- `1156664`
- `1156662`
- `1156661`
- `1182643`

## Initial data ingestion

We use the `cricsheet_data_ingestion.py` script to download the data from Cricsheet and save it to BigQuery tables. The BigQuery data and its underlying data lake is created using Terraform whose scripts are placed in the `terraform` directory. 

Data for all matches in inserted into two BigQuery tables:

- `match_info`: match info for each match with an additional match ID column.
- `ball_data`: ball-by-ball data for each match differentiated by the match ID column.

Some data quirks handled in the ingestion script include:

- While most match IDs, the primary key in both tables, are integers, some have the format `wi_{int}`. Hence, match IDs are saved as strings in both tables.
- Columns `other_wicket_types` and `other_player_dismissed` are often missing and thus converted to integers by default while ingesting in BigQuery. Missing values are hence converted to empty strings (`””`).
- Season specification can either be `2014/15` or `2016`. Hence, season is saved as a string.
