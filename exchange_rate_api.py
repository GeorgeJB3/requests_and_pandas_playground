import pyodbc
import requests
import pandas as pd
import datetime as dt
from config import EXCHANGE_RATE_API_KEY, DB_PASSWORD

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

# Export data to a CSV file to be uploaded to a T-SQL database
df.to_csv("data/clean_exchange_rates.csv", index=False)


# Run the below commands in Azure Data Studio to create a database and a table. Then bulk insert the data

# -- -- Create a new database
# -- CREATE DATABASE exchange_rate_DB;
# -- GO

# -- -- Enable advanced options and BULK INSERT
# -- USE exchange_rate_DB;
# -- GO

# -- Create table if not exists
# IF OBJECT_ID('exchange_rates', 'U') IS NULL
# BEGIN
#     CREATE TABLE exchange_rates (
#         currency VARCHAR(10),
#         exchange_rate FLOAT,
#         exchange_rate_percent FLOAT,
#         ingestion_timestamp DATETIME
#     );
# END
# GO

# -- Load the data from CSV
# BULK INSERT exchange_rates
# FROM '/var/opt/mssql/data/clean_exchange_rates.csv'
# WITH (
#     FIRSTROW = 2,
#     FIELDTERMINATOR = ',',
#     ROWTERMINATOR = '\n',
#     TABLOCK,
#     FORMAT = 'CSV'
# );
# GO