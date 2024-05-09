#!/usr/bin/env python3
import duckdb
import time

DB_NAME = "words.duckdb"

INPUT_FILES = [
    # TODO(jacob): Your files here.
    f"~/Videos/test_data/go_file_{i}.txt" for i in range(61)
]

con = duckdb.connect(DB_NAME)

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

# TODO
