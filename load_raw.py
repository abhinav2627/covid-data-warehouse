import snowflake.connector
import pandas as pd
import requests
import io

# Download COVID data
print('Downloading COVID data...')
url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv'
response = requests.get(url)
df = pd.read_csv(io.StringIO(response.text))

# Keep relevant columns only
cols = [
    'iso_code', 'continent', 'location', 'date',
    'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
    'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
    'population', 'median_age', 'gdp_per_capita'
]
df = df[cols]
df = df[df['continent'].notna()]
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
df = df.fillna(0)

print(f'Rows to load: {len(df):,}')

# Connect to Snowflake — update credentials below
conn = snowflake.connector.connect(
    account='YOUR_ACCOUNT',        # e.g. qfc45176.us-east-1
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    warehouse='COVID_WH',
    database='COVID_DW',
    schema='RAW',
    role='ACCOUNTADMIN'
)
cur = conn.cursor()

# Create raw table
cur.execute('''
    CREATE OR REPLACE TABLE COVID_DW.RAW.RAW_COVID_DATA (
        ISO_CODE                VARCHAR,
        CONTINENT               VARCHAR,
        LOCATION                VARCHAR,
        DATE                    DATE,
        TOTAL_CASES             FLOAT,
        NEW_CASES               FLOAT,
        TOTAL_DEATHS            FLOAT,
        NEW_DEATHS              FLOAT,
        TOTAL_VACCINATIONS      FLOAT,
        PEOPLE_VACCINATED       FLOAT,
        PEOPLE_FULLY_VACCINATED FLOAT,
        POPULATION              FLOAT,
        MEDIAN_AGE              FLOAT,
        GDP_PER_CAPITA          FLOAT
    )
''')
print('RAW table created')

# Load data in chunks
chunk_size = 5000
total = 0
for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i+chunk_size]
    rows = [tuple(row) for row in chunk.itertuples(index=False)]
    cur.executemany(
        'INSERT INTO COVID_DW.RAW.RAW_COVID_DATA VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
        rows
    )
    total += len(rows)
    print(f'  Loaded {total:,} rows...')

conn.commit()
cur.close()
conn.close()
print(f'Done! {total:,} rows loaded into RAW.RAW_COVID_DATA')
