import pandas as pd
from sqlalchemy import create_engine

username = "root"
password = "password"
host = "127.0.0.1"
port = 3306
database = "big_data_project"
table_name = "tweets"

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

csv_file = 'tweets_cleaned.csv'
chunksize = 10000

first_chunk = True
for chunk in pd.read_csv(csv_file, chunksize=chunksize):
    if first_chunk:
        chunk.to_sql(table_name, engine, if_exists='replace', index=False)
        first_chunk = False
    else:
        chunk.to_sql(table_name, engine, if_exists='append', index=False)

print("Data inserted successfully!")




