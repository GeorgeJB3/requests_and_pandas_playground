import requests
import pandas as pd
import datetime as dt
from config import EXCHANGE_RATE_API_KEY

URL = f"https://v6.exchangerate-api.com/v6/{EXCHANGE_RATE_API_KEY}/latest/"

def retrieve_data(domain):
    '''make the API call to retrieve required data'''
    response = requests.get(f'{domain}GBP')
    response.raise_for_status()
    data = response.json()
    return data

def convert_to_dataframe(data):
    '''Convert the api response to a pandas dataframe to begin the transformation stage of the pipeline '''
    tuple_data = data["conversion_rates"].items()
    df = pd.DataFrame(tuple_data, columns=['Currency', 'Rate'])
    df = df.rename(columns={"Currency": "currency", "Rate": "exchange_rate"})
    return df

def check_for_nulls(dataframe):
    '''Returns True if NULLs are present in the data. False if not'''
    return dataframe.isna().values.any()

def check_for_duplicates(dataframe):
    '''Returns True if duplicates are present in the data. False if not'''
    return dataframe.duplicated().values.any()

def add_columns(dataframe):
    '''
    Add column for exhange rate percentage then round to 1 decimal place.
    Add column for ingestion timestamp to keep track of each row being ingested
    '''
    df = dataframe
    df["exchange_rate_%"] = df["exchange_rate"] * 100
    df["exchange_rate_%"] = df["exchange_rate_%"].round(1)
    df["ingestion_timestamp"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

def find_lowest_and_highest_exchange_rate(dataframe):
    ''' Finding the lowest and highest exchange rate '''
    min_exchange_rate = dataframe["exchange_rate"].min()
    max_exchange_rate = dataframe["exchange_rate"].max()
    print(f"Minimum exchange rate is {min_exchange_rate}")
    print(f"Maximum exchange rate is {max_exchange_rate}")

def export_to_csv(dataframe, path):
    ''' Export data to a CSV file to be uploaded to a T-SQL database '''
    dataframe.to_csv(path, index=False)

def run_pipeline():
    '''execute all the functions to run the pipeline'''
    data = retrieve_data(f"https://v6.exchangerate-api.com/v6/{EXCHANGE_RATE_API_KEY}/latest/")
    df = convert_to_dataframe(data)
    if check_for_nulls(df):
        df = df.drop_na()
    if check_for_duplicates(df):
        df = df.drop_duplicates()
    add_columns(df)
    find_lowest_and_highest_exchange_rate(df)
    export_to_csv(df,"data/clean_exchange_rates.csv")

run_pipeline()




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