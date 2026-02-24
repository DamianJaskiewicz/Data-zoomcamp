import sys
import pandas as pd
print("aguments", sys.argv)

day = int(sys.argv[1])
print(f"Running pipline for day {day}")

df = pd.DataFrame({"day":[1, 2], "number_passenger":[3, 4]})
df['day'] = day

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")
print(df.head())