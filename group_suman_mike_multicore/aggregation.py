import numpy as np
import pandas as pd
import time
import math
from collections import defaultdict
import timeit
import sys
from os import path
sys.path.append(path.abspath(path.join(path.dirname(__file__), '../')))
from group_suman_mike_naive_v2.aggregation import get_vc_pc_po
from concurrent import futures
from multiprocessing import cpu_count


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


def get_vc_pc_po_parallel(df):
    n = cpu_count()
    total_rows = len(df)
    partition_size = math.ceil(total_rows // n)
    dfs = []
    start_index = 0
    for i in range(n - 1):
        dfs.append(df.iloc[start_index:start_index + partition_size, :])
        start_index += partition_size
    # append the remaining
    dfs.append(df.iloc[start_index:, :])
    len_dfs = [len(df_) for df_ in dfs]
    assert sum(len_dfs) == len(df), f"{len_dfs = }"
    with futures.ProcessPoolExecutor(max_workers=8) as pool:
        analyzers = [pool.submit(get_vc_pc_po, df) for df in dfs]
        analyzed = [worker.result() for worker in futures.as_completed(analyzers)]

    vc, pc, po = analyzed[0]
    for example in analyzed[1:]:
        vc_next, pc_next, po_next = example
        for key in vc.keys():
            vc[key] += vc_next[key]
            pc[key] += pc_next[key]
            po[key] += po_next[key]
    return vc, pc, po


def query(df):
    """
    The result dictionary looks like: {f_t: [num_fuel_types, num_passengers, num_non_nan_pass]}
    The num_non_nan_pass is required later when finding the average passenger count.
    """
    result_vc, result_pc, result_po = get_vc_pc_po_parallel(df)
    data = []
    for ft in result_vc.keys():
        temp = (ft, result_vc[ft], round(result_pc[ft] / result_po[ft], 1))
        data.append(temp)
    result_df = pd.DataFrame(columns=['fuel_type', 'vehicle_count', 'avg_passengers'], data=data)
    result_df.sort_values(by="fuel_type", ascending=True, inplace=True)
    result_df.reset_index(inplace=True)
    result_df['fuel_type'] = result_df['fuel_type'].replace(str(math.nan), math.nan)
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