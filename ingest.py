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

#
# 1.Ingest the raw data.
#

# Improves memory usage when importing files larger than memory.
# I don't think the input data is in any order that matters, so insertion
# order shouldn't matter here.
con.sql("SET preserve_insertion_order = false;")

# The table we'll actually query against is going to have a different schema, but
# it seemed wise to store the raw data more-or-less as-is first, and then
# transform it into new tables from there.
# If you're pressed for disk space, there's no reason you couldn't transform the
# data directly into the format needed and store that rather than also using this
# intermediate table.
# TODO(jacob): You'll need to either adjust this schema a bit, or tweak the COPY
# statement below. I didn't understand what all of the columns in the input were
# for, so I only generated the ones you see here.
# You don't _need_ to ingest all of the columns you're not using, but it shouldn't
# slow any queries down once the data is ingested, and I'd suggest doing it on
# principle in case you need it for future shenanigans.
con.sql("CREATE TABLE raw_data (count_by_word JSON, date DATE, publisher TEXT, doc TEXT);")

# In theory one could ingest all the data in one statement, as described here:
# https://duckdb.org/docs/data/multiple_files/overview
# In practice, I used up all 64GB of memory plus 64GB of swap very quickly, even
# after reducing the number of threads.
# Going one file at a time seems to keep the memory down, and is still decently
# speedy. I got ~2 minutes/file when reading from my SATA SSD, and ~1 minute/file
# from the NVMe drive. If you needed it, I suspect you could speed it up more by
# doing a few files at a time.
start_time = time.time()
for filename in INPUT_FILES:
    file_start = time.time()
    con.sql(f'COPY raw_data FROM "{filename}"')
    print(f"ingested {filename} in {time.time() - file_start} seconds")
print(f"Finished in {time.time() - start_time} seconds.")

#
# 2. Transform the data into a more query-friendly schema.
#

# The USMALLINT instead of INT actually should save substantial memory at query time,
# and thus improve the performance. If you think the count for any single word in a doc
# could be more than 65k, feel free to change - earlier iterations used a normal-size
# int and worked fine.
start_time = time.time()
print("Massaging data into query-friendly format...")
con.sql("CREATE TABLE word_counts(date DATE, count USMALLINT, publisher VARCHAR, doc VARCHAR, word VARCHAR);")
con.sql("INSERT INTO word_counts SELECT date, UNNEST(count_by_word->'$.*'), publisher, doc, UNNEST(json_keys(count_by_word)) FROM raw_data;")
print(f"Finished in {time.time() - start_time} seconds.")
