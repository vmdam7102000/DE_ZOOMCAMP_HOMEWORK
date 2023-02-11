import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    
    #load params from cmd
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name_1 = params.table_name_1
    table_name_2 = params.table_name_2
    url_1 = params.url_1
    url_2 = params.url_2

    
    if url_1.endswith('.csv.gz'):
        csv_name_1 = 'output_green_trip.csv.gz'
    else:
        csv_name_1 = 'output_green_trip.csv'

    if url_2.endswith('.csv.gz'):
        csv_name_2 = 'output_zone.csv.gz'
    else:
        csv_name_2 = 'output_zone.csv'

    #get file to host machine
    os.system(f"wget {url_1} -O {csv_name_1}")
    os.system(f"wget {url_2} -O {csv_name_2}")

    #engine to connect to db
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #insert zone data to db
    df_zone = pd.read_csv(csv_name_2)
    df_zone.head(n=0).to_sql(name=table_name_2, con=engine, if_exists='replace')
    df_zone.to_sql(name=table_name_2, con=engine, if_exists='append')

    #read csv by iterator 
    df_iter_green_taxi = pd.read_csv(csv_name_1, iterator=True, chunksize=100000)
    df_green_taxi = next(df_iter_green_taxi)

    #convert text to datetime format
    df_green_taxi.lpep_pickup_datetime = pd.to_datetime(df_green_taxi.lpep_pickup_datetime)
    df_green_taxi.lpep_dropoff_datetime = pd.to_datetime(df_green_taxi.lpep_dropoff_datetime)

    #insert green taxi data to db

    df_green_taxi.head(0).to_sql(name=table_name_1, con=engine, if_exists='replace')
    df_green_taxi.to_sql(name=table_name_1, con=engine, if_exists='append')

    while True: 

        try:
            t_start = time()
            
            df = next(df_iter_green_taxi)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name_1, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name_1', required=True, help='name of the table where we will write the taxi green data to')
    parser.add_argument('--table_name_2', required=True, help='name of the table where we will write the zone data to')
    parser.add_argument('--url_1', required=True, help='url of the taxi green data csv file')
    parser.add_argument('--url_2', required=True, help='url of the zone csv file')

    args = parser.parse_args()

    main(args)