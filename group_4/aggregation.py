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
    count_dict = {}
    passenger_sum_dict = {}
    passenger_count_dict = {}
    df['fuel_type'] = df['fuel_type'].astype(str)
    print(df.info())
    for index, row in df.iterrows():
        # print(row["fuel_type"], row["passengers"])
        if row["fuel_type"] in count_dict:
            count_dict[row["fuel_type"]] += 1
            if pd.isna(row["passengers"]):
                passenger_sum_dict[row["fuel_type"]] += 0
            else:
                passenger_count_dict[row["fuel_type"]] += 1
                passenger_sum_dict[row["fuel_type"]] += int(row["passengers"])
        else:
            count_dict[row["fuel_type"]] = 1
            passenger_count_dict[row["fuel_type"]] = 0
            if pd.isna(row["passengers"]):
                passenger_sum_dict[row["fuel_type"]] = 0
            else:
                passenger_count_dict[row["fuel_type"]] += 1
                passenger_sum_dict[row["fuel_type"]] = int(row["passengers"])
        if index % 100000 == 0:
            print(index)
    print(count_dict)
    print(passenger_sum_dict)

    key_list = list(count_dict.keys())
    print(key_list)
    # key_list.sort()

    output_list = []
    for key in sorted(key_list):
        count = count_dict[key]
        if passenger_count_dict[key] == 0:
            avg = 'NaN'
        else:
            avg = round(passenger_sum_dict[key] / passenger_count_dict[key], 1)
        if key == "nan":
            key = "NaN"
        output_list.append((key, count, avg))

    print(output_list)
    return pd.DataFrame(columns=['fuel_type', 'vehicle_count', 'avg_passengers'], data=output_list)


# Read data
df = pd.read_csv('dmv_fuel_type_passengers.csv')

# Run query (data is loaded before, everything else needs to be timed)
start = time.time()
result = query(df)
end = time.time()

# Validate result and print time
if validate(result):
    print("Result:", end - start)
else:
    print("Result: Error")
