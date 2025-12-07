# Movie Pipeline — MovieLens + OMDb ETL

## Overview
This project builds a small ETL pipeline that ingests MovieLens CSVs (`movies.csv`, `ratings.csv`), enriches movies with metadata from the OMDb API (director, plot, box office, released date, runtime), and loads the cleaned data into an SQLite database (`movies.db`) for analysis.

## Repo structure

## step-1
git clone https://github.com/Ravitejagandluri/movie-pipeline.git
cd movie-pipeline

## step-2
python -m venv venv
venv\Scripts\activate.ps1

## step-3
pip install -r requirements.txt
python -m pip install python-dotenv


## step-4  make sure you update with your key or which is working in .env file 
OMDB_API_KEY=your_api_key_here

## step-5 run all the below commands to get the output
python etl.py
python enrich_missing.py
python run_queries.py

movie-pipeline/
│
├── etl.py
├── enrich_missing.py
├── run_queries.py
├── schema.sql
├── queries.sql
├── .env
├── requirements.txt
└── movies.db (generated automatically after running etl.py)

