import pandas as pd
import time


def validate(actual_result):
    expected_result = pd.read_csv('dmv_fuel_type_passengers_expected.csv')
    # print(type(expected_result["fuel_type"][8]))
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
    fuel_type = df['fuel_type'].to_numpy()
    passengers = df['passengers'].to_numpy()
    # print(df.info())
    for i in range(len(fuel_type)):
        # print(row["fuel_type"], row["passengers"])
        if fuel_type[i] in count_dict:
            count_dict[fuel_type[i]] += 1
            if pd.isna(passengers[i]):
                passenger_sum_dict[fuel_type[i]] += 0
            else:
                passenger_count_dict[fuel_type[i]] += 1
                passenger_sum_dict[fuel_type[i]] += int(passengers[i])
        else:
            count_dict[fuel_type[i]] = 1
            passenger_count_dict[fuel_type[i]] = 0
            if pd.isna(passengers[i]):
                passenger_sum_dict[fuel_type[i]] = 0
            else:
                passenger_count_dict[fuel_type[i]] += 1
                passenger_sum_dict[fuel_type[i]] = int(passengers[i])
        # if i % 100000 == 0:
        #     print(i)
    # print(count_dict)
    # print(passenger_sum_dict)

    key_list = list(count_dict.keys())
    # print(key_list)
    # key_list.sort()

    output_list = []
    for key in sorted(key_list):
        count = count_dict[key]
        if passenger_count_dict[key] == 0:
            avg = None
        else:
            avg = round(passenger_sum_dict[key] / passenger_count_dict[key], 1)
        if key == "nan":
            key = None
        output_list.append((key, count, avg))

    # print(output_list)
    return pd.DataFrame(columns=['fuel_type', 'vehicle_count', 'avg_passengers'], data=output_list)


# Read data
df = pd.read_csv('dmv_fuel_type_passengers.csv')

# Run query (data is loaded before, everything else needs to be timed)
start = time.time()
result = query(df)
end = time.time()
# print("Result:", end - start)

# Validate result and print time
if validate(result):
    print("Result:", end - start)
else:
    print("Result: Error")
