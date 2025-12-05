#!/usr/bin/env python3
"""
run_queries.py

Run all queries from queries.sql against movies.db and print results.
Usage: python run_queries.py
"""
import sqlite3
from pathlib import Path
import textwrap

DB = "movies.db"
Q = "queries.sql"

if not Path(DB).exists():
    raise SystemExit("movies.db not found in current folder.")
if not Path(Q).exists():
    raise SystemExit("queries.sql not found in current folder.")

sql_text = Path(Q).read_text(encoding="utf8")

# Simple statement splitter on semicolons (assumes queries.sql does not contain semicolons inside literals)
stmts = [s.strip() for s in sql_text.split(';') if s.strip()]

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

for i, stmt in enumerate(stmts, 1):
    print("\n" + "-" * 60)
    print(f"--- Query {i} ---")
    print(textwrap.indent(stmt, "    "))
    print("-" * 60)
    try:
        cur.execute(stmt)
        rows = cur.fetchall()
        if not rows:
            print("(no rows returned)")
            continue
        headers = rows[0].keys()
        # print header row
        header_line = " | ".join(headers)
        print(header_line)
        print("-" * len(header_line))
        for r in rows:
            print(" | ".join(str(r[h]) for h in headers))
    except Exception as e:
        print("Error running query:", e)

conn.close()
print("\nDone.")
