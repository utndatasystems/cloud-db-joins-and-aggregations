import os
import sys
from subprocess import Popen, PIPE
from datetime import datetime, timedelta
from tabulate import tabulate

# Run with:
# For aggregation: python3 gather_results.py aggregation `ls -d group_*`
# For join: python3 gather_results.py join `ls -d group_*`

# Get the file to be executed
bench_type = sys.argv[1]
group_folders = sys.argv[2:]

results = []

# Loop over all directories starting with "group_"
for folder in group_folders:
    # Execute the file
    file = f'{folder}/{bench_type}.py'
    if not os.path.exists(file):
        continue
    print("Evaluating:", file)
    process = Popen(["python3", file], stdout=PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()

    # Extract result
    text = output.decode("utf-8")
    lines = [iter for iter in text.splitlines() if iter.startswith("Result:")]
    if len(lines) != 1 or "Result" not in lines[0]:
        print("Invalid output format")
        results.append((float('inf'), folder))
        continue
    print(lines[0])

    # Append result to our result array
    try:
        results.append((float(lines[0].split(' ')[1]), folder))
    except:
        results.append((float('inf'), folder))

# Sort and print results
results.sort(key=lambda x: x[0])
formatted_results = []
for idx, result in enumerate(results):
    formatted_results.append([idx+1, str(result[1]), str(result[0])])

print(tabulate(formatted_results,
               headers=['Rank', 'Group', 'Time (seconds)']))
