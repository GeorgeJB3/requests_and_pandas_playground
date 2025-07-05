import requests
import pandas as pd
import datetime as dt
from config import EXCHANGE_RATE_API_KEY

URL = f"https://v6.exchangerate-api.com/v6/{EXCHANGE_RATE_API_KEY}/latest/"

response = requests.get(f'{URL}GBP')
response.raise_for_status()

data = response.json()
tuple_data = data["conversion_rates"].items()

df = pd.DataFrame(tuple_data, columns=['Currency', 'Rate'])

df = df.rename(columns={"Currency": "currency", "Rate": "exchange_rate"})

# check for NULL values in the data set - False
print(f'Contains NULLs: {df.isna().values.any()}')

# check for duplicates in the data set - False
print(f'Contains Duplicates: {df.duplicated().values.any()}')

# Add new column for rate % then round to 1 decimal place
df["exchange_rate_%"] = df["exchange_rate"] * 100
df["exchange_rate_%"] = df["exchange_rate_%"].round(1)

# Add an ingestion timestamp column so we know when the data was ingested
df["ingestion_timestamp"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

min_exchange_rate = df["exchange_rate"].min()
max_exchange_rate = df["exchange_rate"].max()

print(f"Minimum exchange rate is {min_exchange_rate}")
print(f"Maximum exchange rate is {max_exchange_rate}")


print(df.head())


