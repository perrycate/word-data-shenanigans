#!/usr/bin/env python3
import duckdb
import time

DB_NAME = "words.duckdb"

INPUT_FILES = [
    # TODO(jacob): Your files here.
    f"~/Videos/test_data/go_file_{i}.txt" for i in range(61)
]

con = duckdb.connect(DB_NAME)

# TODO(jacob):
# Step 0: Transform the data to valid csv/json.
# My lazy self generated the data as valid JSON contained in a csv file.
# (so no '[' or ']' at the beginning/end of the line, the word counts were JSON
# instead of a string-formatted python dict, etc.) Iirc you already had some code
# to do this, so if you haven't saved the results of it already you'll probably
# want to give that a go.

# Improves memory usage when importing files larger than memory.
# I don't think the input data is in any order that matters, so insertion
# order shouldn't matter here.
con.sql("SET preserve_insertion_order = false;")


# The USMALLINT instead of INT actually should save substantial memory at query time,
# and thus improve the performance. If you think the count for any single word in a doc
# could be more than 65k, feel free to change - earlier iterations used a normal-size
# int and worked fine.
con.sql("CREATE TABLE word_counts(date DATE, cnt USMALLINT, publisher TEXT, doc TEXT, word TEXT);")

con.close()

# This takes about 4 hours on my machine. With inputs totalling ~1TB across 61 files,
# it produces a db file that's ~500GB.
start_time = time.time()
print("Ingesting records...")
for in_file in INPUT_FILES:
    print(f"Reading {in_file}... ", end='')

    # There seems to be a memory leak in duckdb's python library.
    # If we use the same connection for each insert, we eventually run out of memory.
    # Closing/reopening is a stupid but functioning workaround.
    con = duckdb.connect(DB_NAME)
    file_start = time.time()

    con.sql(f"""
    INSERT INTO word_counts SELECT
        column1::DATE as date,
        UNNEST(column0->'$.*')::USMALLINT as cnt,
        column2::TEXT as publisher,
        column3::TEXT as doc,
        UNNEST(json_keys(column0)) as word
    FROM read_csv('{in_file}');""")

    con.close()

    print(f"done ({time.time() - file_start} seconds).")
print(f"Finished in {time.time() - start_time} seconds.")

# You're done!
# Sample query:
#
# Getting the sum of all the counts of a set of words:
# SELECT word, SUM(cnt) FROM word_counts WHERE word in (SELECT word FROM lexicon) GROUP BY word;
# I use gen_lexicon to make a table containing a lexicon, but you could just as easily load the lexicon
# from a csv using read_csv.
