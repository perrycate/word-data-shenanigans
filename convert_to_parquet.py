#!/usr/bin/env python3
"""
Converts the csv input format into a parquet containing "unnested" rows,
one row per (publisher, doc, word).
"""

import duckdb
import sys

if __name__ == '__main__':
    in_file = sys.argv[1]
    out_file = sys.argv[2]

    # Frees up memory during io.
    duckdb.sql("SET preserve_insertion_order=False;")

    duckdb.sql(f"""
    COPY (
        SELECT
            UNNEST(column0->'$.*')::USMALLINT as cnt,
            column1::DATE as date,
            column2::TEXT as publisher,
            column3::TEXT as doc,
            UNNEST(json_keys(column0)) as word
        FROM read_csv('{in_file}')
    ) TO '{out_file}' (FORMAT PARQUET);""")
