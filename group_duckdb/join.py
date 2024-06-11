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
    con.execute("SET threads TO 1;")
    cursor = con.cursor()
    cursor.execute("""select sum(l_extendedprice)::bigint as volume
                      from lineitem, part
                      where l_partkey = p_partkey
                        and l_shipdate >= date '1995-09-01'
                        and l_shipdate < date '1995-10-01';""")
    if cursor.description is None:
        return pd.DataFrame()
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return pd.DataFrame(data=rows, columns=column_names)


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
    "copy lineitem from '/Users/alex/workspace/data/tpch/sf-1/lineitem.csv' CSV;").fetchall()
con.execute(
    "copy part from '/Users/alex/workspace/data/tpch/sf-1/part.csv' CSV;").fetchall()

# Run query (data is loaded before, everything else needs to be timed)
start = time.time()
result = query(con)
end = time.time()

# Validate result and print time
if validate(result):
    print("Result:", end - start)
else:
    print("Result: Error")
