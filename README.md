0. Python dependencies: `pip3 install duckdb pandas tabulate`
1. Download dataset from s3: `s3://alex-datasets/dmv/dmv_fuel_type_passengers.csv`
2. Copy the downloaded dataset into the root directory of the repo.
3. Create a copy of the template (`group_template`), please prefix your group folder with `group_`:
   ```bash
   cp -rf group_template group_lightning_speed
   ```
4. Run your solution:
   ```bash
   python3 group_lightning_speed/aggregation.py
   ```
5. Run all solutions:

   aggregation
   ```bash
   python3 gather_results.py aggregation `ls -d group_*`
   ```
   join
   ```bash
   python3 gather_results.py join `ls -d group_*`
   ```
6. Checkin:
   ```bash
   git add group_lightning_speed
   git commit -m "updated group_lightning_speed"
   git fetch && git rebase origin/main && git push origin main
   ```
