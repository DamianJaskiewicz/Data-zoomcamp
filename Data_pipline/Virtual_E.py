import sys
import pandas as pd
print("aguments", sys.argv)

day = int(sys.argv[1])
print(f"Running pipline for day {day}")

df = pd.DataFrame({"A":[1, 2], "B":[3, 4]})
print(df.head())
df.to_parquet(f"output_day_{sys.argv[1]}.parquet")

