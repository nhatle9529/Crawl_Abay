import json
import pathlib
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.python import PythonOperator,  get_current_context
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import Variable
import re
import pandas as pd
from datetime import date, datetime, timedelta
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.models import Variable
from bs4 import BeautifulSoup


HOOK_MSSQL = 'Conn_ID'
# header = Variable.get('header_abay')
header =  Variable.get('header')
route = Variable.get('route')
url_1 = Variable.get('abay1')
url_2 = Variable.get('abay2')

@dag(
    start_date=datetime.now(),
    schedule_interval='0 8 * * *',
    catchup=False,
)

def Crawl_abay():
    @task()
    def gettoday():
        context = get_current_context()
        dag_run = context["dag_run"]

        print(f"Configuration JSON (Optional): {dag_run.conf}")
        try:
            p1 = dag_run.conf['date_from']
            date_from = datetime.strptime(p1, "%d%b%Y").strftime("%d%b%Y") 
        except:
            today = datetime.today()
            date_from = today.strftime("%d%b%Y")
        return date_from


    @task()
    def create_url(date):
        urls = []
        Date = pd.date_range(date, periods=50).strftime('%d%b%Y').tolist()
        for i in range(len(route)):
            for j in range(len(Date)):
                url = url_1+route[i]+'-1-0-0-'+Date[j]+url_2
                urls.append(url)
        # print(urls)
        return urls
    @task()
    def crawlbay(urls):    
        def get_data(url):
            response = requests.post(url, headers=header)
            # print(response)
            pattern = re.compile(r'input=([A-Z]{3}-[A-Z]{3}).*?(\d{2}[A-Za-z]{3}\d{4})')

            # Search for the pattern in the URL
            match = pattern.search(url)
            flight_route = match.group(1)
            flight_date = match.group(2)

            data = response.json()
            print(data)
            test = json.loads(data['d'])
            if data['d'] == "{}":
                return pd.DataFrame(columns=['Date','etd_hh','flight_nmbr', 'Price','Date Flight','Brand','leg'])
            else:
                df = pd.DataFrame(test['D'])
                df = df.drop(['H','I','K','J','C','O','P','Q','T','R','S','N'], axis=1)
                df.rename(columns = {'E':'Brand', 'F': 'flight_nmbr', 'G': 'Time', 'L': 'Price'}, inplace = True)
                df['etd_hh'] = df['Time'].str[:2]
                df['etd_hh'].astype(int)
                df['leg'] = flight_route 
                df['Date Flight'] = flight_date
                df['Date_Crawl'] = datetime.now().strftime("%Y-%m-%d %H:%M")
                df = df.drop_duplicates()
            return df 
        data_frames = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(get_data, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    if not result.empty:
                        data_frames.append(result)
                except Exception as e:
                    print(f"Error fetching data from {url}: {e}")

        if data_frames:
            final_dataframe = pd.concat(data_frames, ignore_index=True)
            print(final_dataframe)
            return final_dataframe.to_dict(orient='records')
        else:
            return []
   
    @task()
    def Savingfile(file):
        df = pd.DataFrame(file)

        hook = MsSqlHook(HOOK_MSSQL)
        table_name = '[dbo].[CrawlAbay]'
        rows_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]
        # print(df)
        # Convert columns to the correct data types
        df['Date_Crawl'] = df['Date_Crawl'].astype(str)
        df['etd_hh'] = df['etd_hh'].astype(str)
        df['flight_nmbr'] = df['flight_nmbr'].astype(str)

        # Handle the Price column to ensure it only contains numeric values
        df['Price'] = df['Price'].replace('[^0-9.]', '', regex=True)
        # df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df['Price'] = df['Price'].astype(str)
        df['Date_Flight'] = df['Date Flight'].astype(str)
        df['Brand'] = df['Brand'].astype(str)
        df['leg'] = df['leg'].astype(str)
    


        # Prepare the SQL query with placeholders for each column
        columns = ['Brand','flight_nmbr','[Time]', 'Price','etd_hh','leg','Date_Flight','Date_Crawl']

        placeholders = ', '.join(['%s' for _ in columns])
        sql = f"""INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"""
        
        # Execute the query using executemany
        conn = hook.get_conn()
        cursor = conn.cursor()
     
        cursor.executemany(sql, rows_to_insert)
        # Commit the changes
        conn.commit()
        

    task1 = gettoday()
    task2 = create_url(task1)
    task3 = crawlbay(task2)
    task4 = Savingfile(task3)
    task1 >> task2 >> task3 >> task4
CrawlAbay = Crawl_abay()
