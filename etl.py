# etl.py
"""
Final ETL script for MovieLens + OMDb enrichment.

Improvements included:
- Robust OMDb requests with short timeouts, rate-limit detection
- Caching of OMDb responses to omdb_cache.json
- --fast (skip API calls), --limit (process N movies), --verbose flags
- Idempotent DB loads, handles imdb_id UNIQUE collisions
- Progress reporting and timings
- FIX: when using --limit, ratings inserted are filtered to only valid movie_ids
"""

import os
import re
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from requests.exceptions import RequestException, Timeout, ConnectionError
from dotenv import load_dotenv
load_dotenv()



# ==========================
# CONFIG - change if needed
# ==========================
DB_URL = os.environ.get("DB_URL", "sqlite:///movies.db")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_CACHE_FILE = "omdb_cache.json"
MOVIES_CSV = "movies.csv"
RATINGS_CSV = "ratings.csv"
SLEEP_BETWEEN_CALLS = 0.12
# ==========================

OMDB_RATE_LIMITED = False
_session = requests.Session()

def load_cache(path):
    if Path(path).exists():
        return json.loads(Path(path).read_text(encoding="utf8"))
    return {}

def save_cache(cache, path):
    Path(path).write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf8")

def parse_title_and_year(title):
    m = re.match(r"^(?P<title>.*)\s+\((?P<year>\d{4})\)$", str(title))
    if m:
        return m.group("title").strip(), int(m.group("year"))
    return title, None

def query_omdb(title, year=None, cache=None, fast=False):
    """
    Try:
      1) t=title (+year optional)
      2) if not found -> s=title (search) -> use first search result's imdbID to fetch full details (i=)
    """
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

    def _call(params):
        try:
            r = _session.get("http://www.omdbapi.com/", params=params, timeout=(3,6))
            return r.json()
        except Exception as e:
            return {"Response": "False", "Error": str(e)}

    # 1) try title search
    params = {"apikey": OMDB_API_KEY, "t": title}
    if year:
        params["y"] = year
    data = _call(params)
    err = (data.get("Error") or "").lower()
    if data.get("Response") == "True":
        cache[key] = data
        time.sleep(SLEEP_BETWEEN_CALLS)
        return data

    # detect rate-limit / invalid-key
    if "limit" in err or "invalid api key" in err:
        OMDB_RATE_LIMITED = True
        cache[key] = {"Response": "False", "Error": data.get("Error", "rate limited/invalid key")}
        return cache[key]

    # 2) if not found, try a search (s=)
    search_params = {"apikey": OMDB_API_KEY, "s": title, "type": "movie"}
    if year:
        search_params["y"] = year
    search = _call(search_params)
    if search.get("Response") == "True" and search.get("Search"):
        first = search["Search"][0]
        imdb = first.get("imdbID")
        if imdb:
            # fetch full details by id
            detail = _call({"apikey": OMDB_API_KEY, "i": imdb})
            if detail.get("Response") == "True":
                cache[key] = detail
                time.sleep(SLEEP_BETWEEN_CALLS)
                return detail

    # fallback - store original failure
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

def print_progress(current, total, start_time, extra=""):
    elapsed = time.time() - start_time
    rate = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / rate if rate > 0 else float('inf')
    print(f"[{current}/{total}] elapsed={elapsed:.1f}s rate={rate:.2f} rows/s eta={eta:.1f}s {extra}")

def main(limit=None, fast=False, verbose=False):
    t0 = time.time()
    print("Starting ETL...", f"fast_mode={fast}", f"limit={limit}")
    if not Path(MOVIES_CSV).exists() or not Path(RATINGS_CSV).exists():
        raise FileNotFoundError("Place movies.csv and ratings.csv in the project folder before running.")

    cache = load_cache(OMDB_CACHE_FILE)
    movies_df = pd.read_csv(MOVIES_CSV)
    ratings_df = pd.read_csv(RATINGS_CSV)
    print(f"Loaded CSVs: movies={len(movies_df)} ratings={len(ratings_df)} rows")

    # apply limit (for testing) to movies only
    if limit:
        movies_df = movies_df.head(limit)
    total_movies = len(movies_df)

    engine = create_engine(DB_URL, future=True)

    # Apply schema.sql safely (split into statements)
    schema_path = Path("schema.sql")
    if schema_path.exists():
        with engine.begin() as conn:
            sql_text = schema_path.read_text()
            statements = [s.strip() for s in sql_text.split(';') if s.strip()]
            for stmt in statements:
                conn.exec_driver_sql(stmt)

    # Insert / upsert movies and genres
    movie_start = time.time()
    with engine.begin() as conn:
        processed = 0
        last_report = time.time()
        for idx, row in movies_df.iterrows():
            processed += 1
            movie_id = int(row["movieId"])
            raw_title = row["title"]
            genres_raw = row.get("genres", "")
            title, year = parse_title_and_year(raw_title)

            omdb_raw = query_omdb(title, year=year, cache=cache, fast=fast)
            omdb = clean_omdb_response(omdb_raw) if omdb_raw else None

            imdb_id = omdb.get("imdb_id") if omdb else None
            director = omdb.get("director") if omdb else None
            plot = omdb.get("plot") if omdb else None
            box_office = omdb.get("box_office") if omdb else None
            released = omdb.get("released") if omdb else None
            runtime = omdb.get("runtime_minutes") if omdb else None

            upsert_sql = text("""
                INSERT INTO movies (id, title, year, imdb_id, director, plot, box_office, released, runtime_minutes)
                VALUES (:id, :title, :year, :imdb_id, :director, :plot, :box_office, :released, :runtime)
                ON CONFLICT(id) DO UPDATE SET
                  title=excluded.title,
                  year=excluded.year,
                  imdb_id=COALESCE(excluded.imdb_id, movies.imdb_id),
                  director=COALESCE(excluded.director, movies.director),
                  plot=COALESCE(excluded.plot, movies.plot),
                  box_office=COALESCE(excluded.box_office, movies.box_office),
                  released=COALESCE(excluded.released, movies.released),
                  runtime_minutes=COALESCE(excluded.runtime_minutes, movies.runtime_minutes)
            """)

            params = {
                "id": movie_id,
                "title": title,
                "year": year,
                "imdb_id": imdb_id,
                "director": director,
                "plot": plot,
                "box_office": box_office,
                "released": released,
                "runtime": runtime
            }

            try:
                conn.execute(upsert_sql, params)
            except IntegrityError as e:
                msg = str(e).lower()
                if "imdb_id" in msg or "unique constraint failed: movies.imdb_id" in msg:
                    params_no_imdb = params.copy()
                    params_no_imdb["imdb_id"] = None
                    fallback_sql = text("""
                        INSERT INTO movies (id, title, year, imdb_id, director, plot, box_office, released, runtime_minutes)
                        VALUES (:id, :title, :year, :imdb_id, :director, :plot, :box_office, :released, :runtime)
                        ON CONFLICT(id) DO UPDATE SET
                          title=excluded.title,
                          year=excluded.year,
                          director=COALESCE(excluded.director, movies.director),
                          plot=COALESCE(excluded.plot, movies.plot),
                          box_office=COALESCE(excluded.box_office, movies.box_office),
                          released=COALESCE(excluded.released, movies.released),
                          runtime_minutes=COALESCE(excluded.runtime_minutes, movies.runtime_minutes)
                    """)
                    conn.execute(fallback_sql, params_no_imdb)
                else:
                    raise

            # genres
            genre_list = [g for g in str(genres_raw).split("|") if g and g != "(no genres listed)"]
            for g in genre_list:
                conn.execute(text("INSERT OR IGNORE INTO genres (name) VALUES (:name)"), {"name": g})
                res = conn.execute(text("SELECT id FROM genres WHERE name = :name"), {"name": g})
                gid = res.scalar_one_or_none()
                if gid:
                    conn.execute(text("""
                        INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (:movie_id, :genre_id)
                    """), {"movie_id": movie_id, "genre_id": gid})

            now = time.time()
            if verbose or (now - last_report >= 5) or (processed % 200 == 0) or processed == total_movies:
                print_progress(processed, total_movies, movie_start, extra=f"title='{title[:30]}'")
                last_report = now

    save_cache(cache, OMDB_CACHE_FILE)
    movie_time = time.time() - movie_start
    print(f"Movies processed: {total_movies} (took {movie_time:.2f}s)")

    # -------------------------
    # Insert ratings (fixed)
    # -------------------------
    ratings_start = time.time()

    # Build set of valid movie_ids: those already in DB plus those just processed
    with engine.begin() as conn:
        # get existing movie ids from DB
        res = conn.execute(text("SELECT id FROM movies"))
        existing_ids = {row[0] for row in res.fetchall()}

    # Also include movieIds from the movies_df we processed in this run (in case DB was empty prior)
    processed_ids = set(movies_df["movieId"].astype(int).tolist())
    valid_movie_ids = existing_ids.union(processed_ids)

    # Filter ratings to only those whose movieId is in valid_movie_ids
    original_ratings_count = len(ratings_df)
    ratings_df_filtered = ratings_df[ratings_df["movieId"].isin(valid_movie_ids)].copy()
    filtered_count = len(ratings_df_filtered)
    dropped = original_ratings_count - filtered_count
    if dropped > 0:
        print(f"Filtered ratings: dropping {dropped} ratings that reference movies not present in DB (safe for --limit)")

    # Insert filtered ratings
    with engine.begin() as conn:
        inserted = 0
        for r in ratings_df_filtered.itertuples(index=False):
            conn.execute(text("""
                INSERT OR IGNORE INTO ratings (user_id, movie_id, rating, timestamp)
                VALUES (:user_id, :movie_id, :rating, :timestamp)
            """), {
                "user_id": int(r.userId),
                "movie_id": int(r.movieId),
                "rating": float(r.rating),
                "timestamp": int(r.timestamp) if hasattr(r, 'timestamp') and not pd.isna(r.timestamp) else None
            })
            inserted += 1
            if verbose and inserted % 1000 == 0:
                print(f"Inserted {inserted} ratings...")

    ratings_time = time.time() - ratings_start
    total_time = time.time() - t0
    print(f"Ratings inserted: {inserted} (took {ratings_time:.2f}s)")
    print(f"ETL finished. Total time: {total_time:.2f}s. DB at: {DB_URL}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Process only first N movies (for testing)")
    parser.add_argument("--fast", action="store_true", help="Skip OMDb API calls (fast local runs)")
    parser.add_argument("--verbose", action="store_true", help="Verbose progress output")
    args = parser.parse_args()
    main(limit=args.limit, fast=args.fast, verbose=args.verbose)
