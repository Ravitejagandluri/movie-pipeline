# enrich_missing.py
"""
Enrich movies that are missing OMDb metadata (director/imdb_id) but have ratings.
Usage:
  python enrich_missing.py            # default: batch 500 (process up to 500 movies)
  python enrich_missing.py --batch 1000
  python enrich_missing.py --fast     # skip network calls (test only)
  python enrich_missing.py --limit 50 --fast
"""
import argparse
import json
import time
from pathlib import Path
from datetime import datetime

import requests
import sqlite3
import pandas as pd
from dotenv import load_dotenv
load_dotenv()



# CONFIG
DB_PATH = "movies.db"
MOVIES_TABLE = "movies"
OMDB_CACHE_FILE = "omdb_cache.json"
DEFAULT_BATCH = 500
# polite pause (seconds) between calls; reduce for testing, increase for politeness
SLEEP_BETWEEN_CALLS = 0.15
# default key - remove before committing to any public repo; prefer environment variable
import os
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

# runtime flag to avoid repeated rate-limited attempts
OMDB_RATE_LIMITED = False
_session = requests.Session()

def load_cache(path=OMDB_CACHE_FILE):
    p = Path(path)
    if p.exists():
        return json.loads(p.read_text(encoding="utf8"))
    return {}

def save_cache(cache, path=OMDB_CACHE_FILE):
    Path(path).write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf8")

def query_omdb(title, year=None, cache=None, fast=False):
    """Robust OMDb query — returns dict or {"Response":"False","Error":...}"""
    global OMDB_RATE_LIMITED, _session
    if cache is None:
        cache = {}
    key = f"{title}||{year}"
    if key in cache:
        return cache[key]
    if fast or OMDB_RATE_LIMITED:
        cache[key] = {"Response": "False", "Error": "fast-mode or rate-limited: skipped"}
        return cache[key]
    if not OMDB_API_KEY:
        cache[key] = {"Response": "False", "Error": "No API key set"}
        return cache[key]

    params = {"apikey": OMDB_API_KEY, "t": title}
    if year:
        params["y"] = year
    try:
        r = _session.get("http://www.omdbapi.com/", params=params, timeout=(3, 6))
        try:
            data = r.json()
        except ValueError:
            data = {"Response": "False", "Error": f"HTTP {r.status_code}"}
    except requests.exceptions.RequestException as e:
        data = {"Response": "False", "Error": f"Network error: {e}"}

    err = (data.get("Error") or "").lower()
    if "request limit" in err or "limit reached" in err or "invalid api key" in err:
        OMDB_RATE_LIMITED = True
        cache[key] = {"Response": "False", "Error": data.get("Error", "rate limited/invalid key")}
        print(f"[omdb] rate-limited/invalid-key detected: '{data.get('Error')}'. Stopping further OMDb calls this run.")
        return cache[key]

    cache[key] = data
    time.sleep(SLEEP_BETWEEN_CALLS)
    return data

def clean_omdb_response(r):
    if not r or r.get("Response") == "False":
        return None
    imdb_id = r.get("imdbID")
    director = r.get("Director")
    plot = r.get("Plot")
    box_office = r.get("BoxOffice")
    released = r.get("Released")
    runtime = r.get("Runtime")
    runtime_minutes = None
    if runtime and "min" in runtime:
        try:
            runtime_minutes = int(runtime.split()[0])
        except:
            runtime_minutes = None
    released_date = None
    if released and released != "N/A":
        for fmt in ("%d %b %Y", "%Y-%m-%d"):
            try:
                released_date = datetime.strptime(released, fmt).date().isoformat()
                break
            except:
                released_date = None
    return {
        "imdb_id": imdb_id,
        "director": director if director and director != "N/A" else None,
        "plot": plot if plot and plot != "N/A" else None,
        "box_office": box_office if box_office and box_office != "N/A" else None,
        "released": released_date,
        "runtime_minutes": runtime_minutes
    }

def fetch_candidates(conn, batch):
    """
    Return list of (id, title, year) for movies that:
      - have at least 1 rating
      - and (director IS NULL OR imdb_id IS NULL)
    Ordered by number of ratings DESC so we enrich popular movies first.
    """
    cur = conn.cursor()
    # select movie ids that have ratings
    q = f"""
    SELECT m.id, m.title, m.year, COUNT(r.rating) AS cnt
    FROM movies m
    JOIN ratings r ON r.movie_id = m.id
    WHERE (m.director IS NULL OR m.imdb_id IS NULL)
    GROUP BY m.id
    ORDER BY cnt DESC
    LIMIT :batch
    """
    cur.execute(q, {"batch": batch})
    rows = cur.fetchall()
    # rows: list of tuples (id, title, year, cnt)
    return [(r[0], r[1], r[2]) for r in rows]

def update_movie(conn, movie_id, data):
    """
    Update movies table with fields returned by OMDb (upsert style).
    Only update columns if not None.
    """
    cur = conn.cursor()
    fields = []
    params = {"id": movie_id}
    if data.get("imdb_id"):
        fields.append("imdb_id = :imdb_id"); params["imdb_id"] = data["imdb_id"]
    if data.get("director"):
        fields.append("director = :director"); params["director"] = data["director"]
    if data.get("plot"):
        fields.append("plot = :plot"); params["plot"] = data["plot"]
    if data.get("box_office"):
        fields.append("box_office = :box_office"); params["box_office"] = data["box_office"]
    if data.get("released"):
        fields.append("released = :released"); params["released"] = data["released"]
    if data.get("runtime_minutes") is not None:
        fields.append("runtime_minutes = :runtime_minutes"); params["runtime_minutes"] = data["runtime_minutes"]

    if not fields:
        return False
    set_clause = ", ".join(fields)
    sql = f"UPDATE movies SET {set_clause} WHERE id = :id"
    cur.execute(sql, params)
    return True

def main(batch=DEFAULT_BATCH, fast=False):
    if not Path(DB_PATH).exists():
        raise SystemExit(f"{DB_PATH} not found. Run etl.py first.")

    cache = load_cache()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = None
    candidates = fetch_candidates(conn, batch)
    if not candidates:
        print("No candidate movies found (all rated movies already have director/imdb).")
        conn.close()
        return

    print(f"Found {len(candidates)} candidate movies to enrich (batch={batch}). fast={fast}")
    enriched = 0
    skipped = 0
    for movie_id, title, year in candidates:
        if OMDB_RATE_LIMITED and not fast:
            print("[omdb] detected rate-limited state. Stopping.")
            break
        # query cache + API
        omdb_raw = query_omdb(title, year=year, cache=cache, fast=fast)
        omdb = clean_omdb_response(omdb_raw) if omdb_raw else None
        if not omdb:
            skipped += 1
            print(f"Skipped: {title} (no OMDb data or cached error: {omdb_raw.get('Error') if omdb_raw else 'N/A'})")
            continue
        try:
            updated = update_movie(conn, movie_id, omdb)
            conn.commit()
            if updated:
                enriched += 1
                print(f"Enriched: id={movie_id} title='{title}' imdb={omdb.get('imdb_id')} director={omdb.get('director')}")
            else:
                skipped += 1
        except sqlite3.IntegrityError as e:
            # likely imdb_id UNIQUE conflict; skip setting imdb_id for this movie
            print("IntegrityError on update (likely imdb_id collision). Skipping imdb_id for this movie.")
            # attempt to update only non-imdb fields
            if omdb.get("director") or omdb.get("plot") or omdb.get("box_office") or omdb.get("released") or omdb.get("runtime_minutes") is not None:
                data_no_imdb = omdb.copy(); data_no_imdb["imdb_id"] = None
                update_movie(conn, movie_id, data_no_imdb)
                conn.commit()
                enriched += 1
        # save cache frequently
        save_cache(cache)
    conn.close()
    save_cache(cache)
    print(f"Done. Enriched={enriched} skipped={skipped}. Cache saved to {OMDB_CACHE_FILE}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="Number of movies to enrich in this run")
    parser.add_argument("--fast", action="store_true", help="Skip OMDb network calls (test mode)")
    args = parser.parse_args()
    main(batch=args.batch, fast=args.fast)
