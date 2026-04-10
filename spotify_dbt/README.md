# Spotify ELT Pipeline

End-to-end ELT pipeline built with Apache Airflow, dbt, and Snowflake — ingesting
real Spotify Global Top 50 data and transforming it into analytics-ready models.

## Architecture
## Tech Stack

| Tool | Purpose |
|------|---------|
| Spotify Web API | Data source — Global Top 50 tracks, artists, audio features |
| Apache Airflow | Orchestration — weekly schedule, retries, dependency management |
| Snowflake | Cloud data warehouse — RAW, STAGING, MARTS schemas |
| dbt | Transformation — tested, documented, version-controlled SQL models |
| Docker | Local Airflow environment |
| Python | Extract + load scripts |

## Project Structure
## Setup

### Prerequisites
- Docker Desktop
- Snowflake free trial account
- Spotify Developer account

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/spotify-elt-pipeline.git
cd spotify-elt-pipeline
```

### 2. Set up environment variables
```bash
cp .env.example .env
# Fill in your credentials in .env
```

### 3. Start Airflow
```bash
docker compose up airflow-init
docker compose up -d
```

### 4. Connect dbt to Snowflake
```bash
source dbt_venv/bin/activate
cd spotify_dbt
dbt debug
```

### 5. Run the pipeline
```bash
# Trigger manually in Airflow UI at localhost:8080
# Or run dbt directly:
dbt run
dbt test
```

## dbt Models

### Staging layer
- `stg_tracks` — cleaned track data from Spotify API
- `stg_artists` — cleaned artist data with genre and popularity

### Marts layer
- `fct_track_performance` — fact table with weekly chart metrics
- `dim_artists` — artist dimension with genre tags and follower counts

## Data Quality
All dbt models include tests for `not_null`, `unique`, `accepted_values`,
and referential integrity between fact and dimension tables.

## Author
Divyanshi Oberoi — Data Engineering Portfolio Project
EOF