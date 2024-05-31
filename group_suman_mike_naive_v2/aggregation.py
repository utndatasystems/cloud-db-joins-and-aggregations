import numpy as np
import pandas as pd
import time
import math
from collections import defaultdict
import timeit


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

def get_vc_pc_po(df):
    # vehicle_count
    result_vc = defaultdict(int)
    # in pass_count we will collect the sum of passengers and update the occurrence to use in averaging later
    result_pc = defaultdict(int)
    # passenger occurrence
    result_po = defaultdict(int)
    f_t_column = df['fuel_type'].to_numpy()
    pass_column = df['passengers'].to_numpy()
    nan_ft_count = 0
    for i, f_t in enumerate(f_t_column):
        if isinstance(f_t, str):
            result_vc[f_t] += 1
            # update the pass_count value
            if not math.isnan(pass_column[i]):
                result_pc[f_t] += pass_column[i]
                # update the non-nan occurrence
                result_po[f_t] += 1
        else:
            nan_ft_count += 1
    # add the nan_ft_counts
    result_vc[str(math.nan)] = nan_ft_count
    result_pc[str(math.nan)] = math.nan
    result_po[str(math.nan)] = math.nan
    return result_vc, result_pc, result_po


def query(df):
    """
    The result dictionary looks like: {f_t: [num_fuel_types, num_passengers, num_non_nan_pass]}
    The num_non_nan_pass is required later when finding the average passenger count.
    """
    result_vc, result_pc, result_po = get_vc_pc_po(df)
    # print(f"{result_vc = }")
    # print(f"{result_pc = }")
    # print(f"{result_po = }")
    # for ft in result_vc.keys():
        # print(ft, result_vc[ft], type(result_po[ft]))
    data = [(ft, result_vc[ft], round(result_pc[ft]/result_po[ft], 1)) for ft in result_vc.keys()]
    result_df = pd.DataFrame(columns=['fuel_type', 'vehicle_count', 'avg_passengers'], data=data)
    result_df.sort_values(by="fuel_type", ascending=True, inplace=True)
    result_df['fuel_type'] = result_df['fuel_type'].replace(str(math.nan), math.nan)
    result_df.reset_index(inplace=True)
    result_df.drop(["index"], axis=1, inplace=True)
    return result_df

if __name__ == "__main__":
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

    # n = 5
    # avg_time = timeit.timeit(stmt='query(df)', globals=globals(), number=n)
    # print("Average time:", avg_time)