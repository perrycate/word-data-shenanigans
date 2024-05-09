#!/usr/bin/env python3
import json
import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc

# This is the most recent working (albeit barely) script I used to create our
# duckdb database from the raw data. The idea is the same as the newer one: Ingest the raw data,
# then transform it within duckdb into a more queryable format.
# Note that, unlike the newer script, our raw data contains a list of structs with word and count fields,
# rather than JSON containing word fields and count values. I did this because I was having problems
# efficiently transforming the data during earlier ingest attempts.
#
# This script is terrible from a performance standpoint - pyarrow is quite snappy at reading our input
# files, and duckdb does fine reading our batches, but we're CPU bottlenecked on parsing the JSON and
# transforming it into our list of count-word structs. In Python. In a single thread. Oof.
# (Since our "function" definition here is really just instructions for pyarrow on how to construct
# its own function, I was hoping it'd be smart enough to parallelize it within pyarrow. I was wrong.)
#
# I was working on a new ingest script, which is working, but incomplete. I'm gonna test my statement
# for transforming the data and will update it. In the meantime, this script kinda-sorta works.

def transform_line(counts_json: str) -> list:
    return [{"word": w, "count": c} for w, c in json.loads(counts_json).items()]


def to_word_counts(_ctx, counts_json_str):
    return pa.array([transform_line(l) for l in counts_json_str.to_pylist()])


word_counts_schema = pa.list_(
        pa.struct([
            ("count", pa.int64()),
            ("word", pa.string())
        ])
    )

pc.register_scalar_function(
    to_word_counts,
    "to_word_counts",
    {
        "summary": "massages (pivots?) a string containing word counts as json",
        "description": """Transforms a string containing json words and their count values into a
string containing json as "words": [<list of words] and "counts": [<list of corresponding counts].
This will hopefully make transforming the data within duckdb later easier.""",
    },
    { # Input types.
        "counts_json_str": pa.string(),
    },
    # Output type.
    word_counts_schema,
)

if __name__ == '__main__':

    raw_input = ds.dataset(
        'data',
        format="csv",
    )

    reader = pa.RecordBatchReader.from_batches(
        pa.schema([
            ('word_counts', word_counts_schema),
            ('date', pa.string()),
            ('publisher', pa.string())
        ]),
        raw_input.to_batches(columns={
        "word_counts": ds.field('')._call("to_word_counts", [ds.field('word_counts')]),
        "date": ds.field("date"),
        "publisher": ds.field("publisher"),
    }))


    db_con = duckdb.connect('words.duckdb')
    db_con.sql("DROP TABLE IF EXISTS raw_data;")
    db_con.sql("CREATE TABLE raw_data (word_counts STRUCT(count USMALLINT, word TEXT)[], date DATE, publisher TEXT)")
    db_con.sql("INSERT INTO raw_data SELECT * FROM reader;")
