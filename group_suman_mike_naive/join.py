from collections import defaultdict
import datetime

import pandas as pd
import duckdb
import time


def validate(actual_result):
    # Hint: make sure the types of the data frame match as well: print(expected_result["volume"])
    expected_result = pd.read_csv('join_expected.csv')

    if not actual_result.equals(expected_result):
        print("EXPECTED:\n===")
        print(expected_result)
        print("===\nACTUAL:\n===")
        print(actual_result)
        print("===")
        return False
    return True


def query(con):
    # constants for indices
    l_ship_date_idx = 10
    l_extendedprice_idx = 5
    l_partkey_idx = 1
    p_partkey_idx = 0

    # initial volume
    volume = 0

    # load the tables as np arrays
    lineitem = con.table("lineitem").to_df()
    lineitem = lineitem.to_numpy()

    part = con.table("part").to_df()
    part = part.to_numpy()
    # build the hashmap of the small table
    hash_small_table = defaultdict(int)
    for i, p_partkey in enumerate(part[:, p_partkey_idx]):
        if p_partkey not in hash_small_table:
            hash_small_table[p_partkey] = -1

    # probe values from the large table
    for j, l_shipdate in enumerate(lineitem[:, l_ship_date_idx]):
        date = l_shipdate.date()
        l_partkey = lineitem[j, l_partkey_idx]
        if datetime.date(1995, 9, 1) <= date < datetime.date(1995, 10, 1):
            if hash_small_table[l_partkey] == -1:
                volume += lineitem[j, l_extendedprice_idx]
    return pd.DataFrame({'volume': [int(volume)]})


# Read data
con = duckdb.connect(database=':memory:', read_only=False)
schema = """
drop table if exists lineitem;
drop table if exists part;
create table lineitem
(
   l_orderkey      integer        not null,
   l_partkey       integer        not null,
   l_suppkey       integer        not null,
   l_linenumber    integer        not null,
   l_quantity      decimal(12, 2) not null,
   l_extendedprice decimal(12, 2) not null,
   l_discount      decimal(12, 2) not null,
   l_tax           decimal(12, 2) not null,
   l_returnflag    text           not null,
   l_linestatus    text           not null,
   l_shipdate      date           not null,
   l_commitdate    date           not null,
   l_receiptdate   date           not null,
   l_shipinstruct  text           not null,
   l_shipmode      text           not null,
   l_comment       text           not null
);
create table part
(
   p_partkey     integer        not null,
   p_name        text           not null,
   p_mfgr        text           not null,
   p_brand       text           not null,
   p_type        text           not null,
   p_size        integer        not null,
   p_container   text           not null,
   p_retailprice decimal(12, 2) not null,
   p_comment     text           not null
);
"""
con.execute(schema).fetchall()
con.execute(
    "copy lineitem from 'tpch/sf-1/lineitem.csv' CSV HEADER;").fetchall()
con.execute(
    "copy part from 'tpch/sf-1/part.csv' CSV HEADER;").fetchall()

# Run query (data is loaded before, everything else needs to be timed)
start = time.time()
result = query(con)
end = time.time()

# Validate result and print time
if validate(result):
    print("Result:", end - start)
else:
    print("Result: Error")
