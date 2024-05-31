import pandas as pd
import time
import math

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
    """
    The result dictionary looks like: {f_t: [num_fuel_types, num_passengers, num_non_nan_pass]}
    The num_non_nan_pass is required later when finding the average passenger count.
    """
    result = {}
    df_array = df.to_numpy()
    for i, (f_t, p) in enumerate(df_array):
        if isinstance(f_t, str):
            if f_t not in result:  # result[f_t] is None
                if p is not None and not math.isnan(p):
                    result[f_t] = [1, p, 1]
                else:
                    result[f_t] = [1, 0, 0]

            else:
                prev_count = result[f_t][0]
                new_count = prev_count + 1
                prev_pass_count = result[f_t][1]
                prev_pass_non_nan_count = result[f_t][2]
                if p is not None and not math.isnan(p):
                    new_pass_count = prev_pass_count + p
                    result[f_t] = [new_count, new_pass_count, prev_pass_non_nan_count + 1]
                else:
                    result[f_t] = [new_count, prev_pass_count, prev_pass_non_nan_count]
        else:
            if str(math.nan) not in result:
                result[str(math.nan)] = [1, str(math.nan), 1]
            else:
                prev_count = result[str(f_t)][0]
                result[str(math.nan)] = [prev_count + 1, math.nan, 1]
    print(f"{result = }")
    # nnpc - non nan passenger count
    data = [(ft, vc, round(pc/nnpc, 1)) for ft, (vc, pc, nnpc) in zip(result.keys(), result.values())]
    result_df = pd.DataFrame(columns=['fuel_type', 'vehicle_count', 'avg_passengers'], data=data)
    result_df["fuel_type"] = result_df["fuel_type"].replace('nan', math.nan)
    result_df["avg_passengers"] = result_df["avg_passengers"].replace('nan', math.nan)
    result_df.sort_values(by="fuel_type", ascending=True, inplace=True)
    result_df.reset_index(inplace=True)
    result_df.drop(["index"], axis=1, inplace=True)
    return result_df


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
