import requests
import json
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

SNOWFLAKE_ACCOUNT  = "vo17647.us-east-2.aws"
SNOWFLAKE_USER     = "DOBEROI1"
SNOWFLAKE_KEY_PATH = os.path.expanduser("~/.dbt/rsa_key.p8")

def extract():
    print("Extracting top tracks from iTunes API...")
    tracks_raw   = []
    features_raw = []
    artists_raw  = []
    seen_tracks  = set()
    seen_artists = set()

    # iTunes top charts — completely free, no auth
    genres = [
        "https://itunes.apple.com/us/rss/topsongs/limit=50/json",
        "https://itunes.apple.com/gb/rss/topsongs/limit=50/json",
    ]

    for url in genres:
        resp = requests.get(url)
        feed = resp.json()["feed"]["entry"]

        for entry in feed:
            track_id   = entry["id"]["attributes"]["im:id"]
            track_name = entry["im:name"]["label"]
            artist_name = entry["im:artist"]["label"]
            album_name  = entry.get("im:collection", {}).get("im:name", {}).get("label", "")
            genre       = entry["category"]["attributes"]["label"]
            price       = entry.get("im:price", {}).get("attributes", {}).get("amount", "0")
            release_date = entry.get("im:releaseDate", {}).get("label", "")[:10]

            if track_id not in seen_tracks:
                seen_tracks.add(track_id)
                tracks_raw.append({
                    "track_id":     track_id,
                    "track_name":   track_name,
                    "artist_name":  artist_name,
                    "album_name":   album_name,
                    "genre":        genre,
                    "release_date": release_date,
                    "price":        float(price) if price else 0.0,
                    "chart_market": url.split("/")[3],
                    "extracted_at": datetime.utcnow().isoformat()
                })

            artist_key = f"{artist_name}_{url.split('/')[3]}"
            if artist_key not in seen_artists:
                seen_artists.add(artist_key)
                artists_raw.append({
                    "artist_name":  artist_name,
                    "genre":        genre,
                    "chart_market": url.split("/")[3],
                    "extracted_at": datetime.utcnow().isoformat()
                })

    print(f"Extracted {len(tracks_raw)} tracks, {len(artists_raw)} artists")
    return tracks_raw, artists_raw


def get_snowflake_connection():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account          = SNOWFLAKE_ACCOUNT,
        user             = SNOWFLAKE_USER,
        private_key_file = SNOWFLAKE_KEY_PATH,
        warehouse        = "SPOTIFY_WH",
        database         = "SPOTIFY_DB",
        schema           = "RAW",
        role             = "ACCOUNTADMIN"
    )
    # Force the session context immediately after connecting
    cur = conn.cursor()
    cur.execute("USE ROLE ACCOUNTADMIN")
    cur.execute("USE WAREHOUSE SPOTIFY_WH")
    cur.execute("USE DATABASE SPOTIFY_DB")
    cur.execute("USE SCHEMA SPOTIFY_DB.RAW")
    cur.close()
    print("Snowflake connected!")
    return conn


def create_raw_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE OR REPLACE TABLE SPOTIFY_DB.RAW.RAW_TRACKS (
            track_id     VARCHAR,
            track_name   VARCHAR,
            artist_name  VARCHAR,
            album_name   VARCHAR,
            genre        VARCHAR,
            release_date VARCHAR,
            price        FLOAT,
            chart_market VARCHAR,
            extracted_at VARCHAR,
            loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    cur.execute("""
        CREATE OR REPLACE TABLE SPOTIFY_DB.RAW.RAW_ARTISTS (
            artist_name  VARCHAR,
            genre        VARCHAR,
            chart_market VARCHAR,
            extracted_at VARCHAR,
            loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    cur.close()
    print("Tables created!")


def load(conn, tracks, artists):
    cur = conn.cursor()

    def insert_rows(table, columns, rows):
        if not rows: return
        sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})"
        cur.executemany(sql, [tuple(r[c] for c in columns) for r in rows])
        print(f"Loaded {len(rows)} rows → {table}")

    insert_rows("SPOTIFY_DB.RAW.RAW_TRACKS",
        ["track_id","track_name","artist_name","album_name",
         "genre","release_date","price","chart_market","extracted_at"],
        tracks)

    insert_rows("SPOTIFY_DB.RAW.RAW_ARTISTS",
        ["artist_name","genre","chart_market","extracted_at"],
        artists)

    conn.commit()
    cur.close()
    print("All data loaded!")


def verify(conn):
    cur = conn.cursor()
    for t in ["RAW_TRACKS", "RAW_ARTISTS"]:
        cur.execute(f"SELECT COUNT(*) FROM SPOTIFY_DB.RAW.{t}")
        print(f"{t}: {cur.fetchone()[0]} rows")
    cur.close()


if __name__ == "__main__":
    print("="*50)
    print("MUSIC ELT PIPELINE — EXTRACT & LOAD")
    print("="*50)
    tracks, artists = extract()
    conn = get_snowflake_connection()
    create_raw_tables(conn)
    load(conn, tracks, artists)
    print("\nVerification:")
    verify(conn)
    conn.close()
    print("\nDone!")