# YouTube Data Engineering Project

This repository contains YouTube datasets for multiple countries (CSV files) and associated category JSON files suitable for data engineering exercises (bronze/silver/gold layers).

Data layout
- `Data/` — CSV files per country (e.g. `USvideos.csv`, `INvideos.csv`, etc.) and corresponding category JSON files.

Bucket names (from `scripts/info.md`)
- Bronze: `data-eng-project-yt-data-bronze-layer`
- Silver: `data-eng-project-yt-data-silver-layer`
- Gold: `data-eng-project-yt-data-gold-layer`
- Scripts: `data-eng-project-yt-data-script`

Quick next steps
- Add a `requirements.txt` or environment spec if you want reproducible processing.
- Add ETL scripts in `scripts/` to load, clean, and transform CSVs into the silver/gold layers.

If you'd like, I can scaffold a simple ETL script and a `requirements.txt` now.
