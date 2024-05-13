#!/usr/bin/env python3
import duckdb
import time

from convert_to_parquet import extract_word_counts_as_parquet

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

#
# Step 1: Convert data to more usable format.
#
# This takes the .csv files, extracts each key-value pair from the included json,
# and creates parquet files where each row in the resulting parquet file is
# 1 key:value pair in the input json.
# We store the results in .parquet files for space efficiency.
#
# We could parallelize this, but at least on my machine this is already CPU-bound.
start_time = time.time()
print("Extracting word:count records and saving as .parquet files...")
for i in range(len(INPUT_FILES)):
    in_file = INPUT_FILES[i]
    print(f"Extracting {in_file}... ", end='')
    file_start = time.time()
    extract_word_counts_as_parquet(con, in_file, f'extracted_{i}.parquet')
    print(f"done ({time.time() - file_start} seconds).")
print(f"Finished in {time.time() - start_time} seconds.")

#
# Step 2: Read data into the database.
#

# The USMALLINT instead of INT actually should save substantial memory at query time,
# and thus improve the performance. If you think the count for any single word in a doc
# could be more than 65k, feel free to change - earlier iterations used a normal-size
# int and worked fine.
con.sql("CREATE TABLE word_counts(date DATE, cnt USMALLINT, publisher TEXT, doc TEXT, word TEXT);")
start_time = time.time()
print("Ingesting data...")
# TODO test this.
con.sql("INSERT INTO word_counts SELECT date, cnt, publisher, doc, word FROM './*.parquet';")
print(f"Finished in {time.time() - start_time} seconds.")
