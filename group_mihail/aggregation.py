import pandas as pd
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


def query(df):
    # TODO: Implement the query and return a data frame with the result.
    # select fuel_type,
    #        count(*) as vehicle_count,
    #        round(avg(passengers), 1) as avg_passengers
    # from dmv group by fuel_type
    # order by fuel_type;
    return pd.DataFrame(columns=['fuel_type', 'vehicle_count', 'avg_passengers'], data=[('X', 0, 0)])


# Read data
df = pd.read_csv('dmv_fuel_type_passengers_sample.csv')

# Run query (data is loaded before, everything else needs to be timed)
start = time.time()
time.sleep(2)
result = query(df)
end = time.time()

# Validate result and print time
if validate(result):
    print("Result:", end - start)
else:
    print("Result: Error")
