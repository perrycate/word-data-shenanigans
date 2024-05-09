#!/usr/bin/env python3
import duckdb
import time

con = duckdb.connect("words.duckdb")

con.sql("SET preserve_insertion_order = false;")
con.sql("CREATE TABLE raw_data (count_by_word JSON, date DATE, publisher TEXT, doc TEXT);")

start_time = time.time()
for i in range(61):
    file_start = time.time()
    filename = f"~/Videos/test_data/go_file_{i}.txt"
    con.sql(f'COPY raw_data FROM "{filename}"')
    print(f"ingested {filename} in {time.time() - file_start} seconds")
print(f"Finished in {time.time() - start_time} seconds.")
