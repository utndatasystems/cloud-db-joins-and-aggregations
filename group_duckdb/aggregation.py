import pandas as pd
import duckdb
import time


def validate(actual_result):
    expected_result = pd.read_csv('dmv_fuel_type_passengers_expected.csv')
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
    cursor.execute("""select fuel_type,
                      count(*) as vehicle_count,
                      round(avg(passengers), 1) as avg_passengers
            from dmv group by fuel_type
            order by fuel_type;""")
    if cursor.description is None:
        return pd.DataFrame()
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return pd.DataFrame(data=rows, columns=column_names)


# Read data
con = duckdb.connect(database=':memory:', read_only=False)
schema = """
drop table if exists dmv;
create table dmv
(
   fuel_type            text,
   passengers           int
);
"""
con.execute(schema).fetchall()
con.execute("copy dmv from 'dmv_fuel_type_passengers.csv' CSV HEADER;").fetchall()

# Run query (data is loaded before, everything else needs to be timed)
start = time.time()
result = query(con)
end = time.time()

# Validate result and print time
if validate(result):
    print("Result:", end - start)
else:
    print("Result: Error")
