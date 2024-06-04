import pandas as pd
import duckdb
import time


def validate(actual_result):
    expected_result = pd.read_csv('join_expected.csv')
    # Hint: make sure the types of the data frame match as well: print(expected_result.dtypes)
    if not actual_result.equals(expected_result):
        print("EXPECTED:\n===")
        print(expected_result)
        print("===\nACTUAL:\n===")
        print(actual_result)
        print("===")
        return False
    return True


def query(con):
    # TODO: Implement the query and return a data frame with the result.
    # NOTE: Don't use duckdb or built-in pandas functions for the join!
    # NOTE: Your join operator should be done manually.
    #    select sum(l_extendedprice)::bigint as volume
    #    from lineitem, part
    #    where l_partkey = p_partkey
    #    and l_shipdate >= date '1995-09-01'
    #    and l_shipdate < date '1995-10-01';""")
    print("Strat query")
    lineitem_df = con.execute("SELECT * FROM lineitem WHERE l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'").fetchdf()
    part_df = con.execute("SELECT * FROM part").fetchdf()

    part_keys = set(part_df['p_partkey'])

    print(lineitem_df.info())
    print(part_df.info())
    # lineitem_df['l_shipdate'] = pd.to_datetime(lineitem_df['l_shipdate'])
    # lineitem_df['l_shipdate_year'] = lineitem_df['l_shipdate'].dt.year
    # lineitem_df['l_shipdate_month'] = lineitem_df['l_shipdate'].dt.month
    # date_start = pd.to_datetime('1995-09-01')
    # date_end = pd.to_datetime('1995-10-01')
    join_result = []
    # l_partkey_dict = {}
    for index, lineitem_row in lineitem_df.iterrows():
        # if lineitem_row['l_shipdate_year'] == 1995 and lineitem_row['l_shipdate_month'] == 9:
        #     l_partkey_dict[lineitem_row['l_partkey']] = 0
        if lineitem_row['l_partkey'] in part_keys:
            join_result.append(lineitem_row['l_extendedprice'])
        if index % 10 == 0:
            print(index)

    # for index, part_row in part_df.iterrows():
    #     if part_row['p_partkey'] in part_keys:
    #         join_result.append(part_row['l_extendedprice'])
    #     if index % 10000 == 0:
    #         print(index)
    volume = int(sum(join_result))
    return pd.DataFrame({'volume': [volume]})


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
print("fetch schema")
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
