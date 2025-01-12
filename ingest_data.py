#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
import os
from time import time
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'parquet_file.parquet'
    csv_name = 'output.csv'
    #I had to do this with parquet data b/c the source didn't have csv only parquet now
    #download the parquet file
    os.system(f"wget {url} -O {parquet_name}")
    #convert to csv file
    df = pd.read_parquet(parquet_name) #load parquet file in to df
    df.to_csv(csv_name) #convert parquet to csv

    #os.system(f"wget {url}  -O {csv_name}") 

    #create connection engine
    #engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')    
    #df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    #casts this field to datetime data type
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #creates the table with the headers
    #df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    #this will iterate through rows and append data
    #df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    #engine.connect()
    #df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
    #loop for appending chunk size 100,000 lines until complete
    while True: 
        t_start = time()
        df = next(df_iter)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('inserted another block..., took %.3f seconds' % (t_end - t_start) )
        

if __name__ == "__main__":

    #use this to allow for use of variables for all important data
    parser = argparse.ArgumentParser(description="ingest csv data to postgres")
    #user, password, host, port ,database, table name, url of csv

    parser.add_argument('--user', help="username for postgres")
    parser.add_argument('--password', help="password for postgres")
    parser.add_argument('--host', help="host for postgres")
    parser.add_argument('--port', help="port for postgres")
    parser.add_argument('--db', help="db name for postgres")
    parser.add_argument('--table_name', help="name of the table to write results to")
    parser.add_argument('--url', help="url of the parquet file")
    
    args = parser.parse_args()
    
    main(args)








