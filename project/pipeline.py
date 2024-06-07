#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
from pathlib import Path
import requests
from tqdm import tqdm
import pandas as pd
from bs4 import BeautifulSoup
import time
from IPython.display import display

def download(url: str, fname: str, chunk_size=1024):
    resp = requests.get(url, stream=True)
    print(f'url:{url}')
    total = int(resp.headers.get('content-length', 0))
    with open(fname, 'wb') as file, tqdm(
        desc=fname,
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in resp.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)


def fetch_html(url:str):
    resp = requests.get(url)
    resp.raise_for_status()
    
    return resp.content

def fetch_all_hyperlinks(link):
    html_content = fetch_html(link)
    soup = BeautifulSoup(html_content,'html.parser')
    return soup.find_all('a')

def read_all_csv_files(csv_directory,**kwargs):
    csv_directory = Path(csv_directory)
    display(f'Read all csv files in {csv_directory}')
    csv_files = [f for f in csv_directory.iterdir() if f.is_file()]

    
    display(f'Found {len(csv_files)} csv files')

    data_frame_list = []
    for csv_file in tqdm(csv_files):
        data_frame_list.append(pd.read_csv(csv_file,**kwargs))
    
    display(f'Done read')
    
    return data_frame_list
    


if not os.path.exists('../data/download'):
    os.makedirs('../data/download')


for i in range(1,6):
    if not os.path.exists(f'../data/download/data{i}'):
        os.makedirs(f'../data/download/data{i}')


data1_path = '../data/download/data1/data1.csv'
data2_directory = '../data/download/data2'
data3_directory = '../data/download/data3'
data4_directory = '../data/download/data4'
data5_path = '../data/download/data5/data5.csv'


# ### Extract data source 1

# In[ ]:


link1 = 'https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv'


def extract_data1(link):
    try:
        save_position = data1_path
        display('Extracting data source 1')
        download(link,save_position)
    except Exception as e:
        display('Extraction 1 fails')
        display(e)
    else:
        display('Done Extracting data source 1')

extract_data1(link1)


# ### Extract data source 2

# In[ ]:


link2 = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean'


def extract_data2(link):
    print('extract datasource 2')

    try:
        a_tags = fetch_all_hyperlinks(link)
        csv_names = [a_tag['href'] for a_tag in a_tags if a_tag.text.endswith('.txt')]

        display(f'Found {len(csv_names)} CSV files in {link}')
        display(csv_names)

        for csv_name in csv_names:

            download(f'{link}/{csv_name}',f'{data2_directory}/{csv_name}')
    except Exception as e:
        display('Extraction 2 fails')
        display(e)
    else:
        display('Done extracting data 2')

extract_data2(link2)


# ### Extract data source 3

# In[ ]:


link3 = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/precipitation'


def extract_data3(link):
    print('extract datasource 3')

    try:
        a_tags = fetch_all_hyperlinks(link)
        csv_names = [a_tag['href'] for a_tag in a_tags if a_tag.text.endswith('.txt')]

        display(f'Found {len(csv_names)} CSV files in {link}')
        display(csv_names)

        for csv_name in csv_names:
            download(f'{link}/{csv_name}',f'{data3_directory}/{csv_name}')
    except Exception as e:
        display('Extraction 3 fails')
        display(e)
    else:
        display('Done extracting data 3')


extract_data3(link3)


# ### Extract Data source 4

# In[ ]:


link4 = 'https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/monthly/historical'


def extract_data4(link):
    display('extract datasource 4')

    try:
        a_tags = fetch_all_hyperlinks(link)
        gz_names = [a_tag['href'] for a_tag in a_tags if a_tag['href'].endswith('.gz') and 'v2' in a_tag['href']]

        display(f'Found {len(gz_names)} GZ files in {link}')
        display(gz_names)

        for gz_name in tqdm(gz_names):
            time.sleep(1)
            download(f'{link}/{gz_name}',f'{data4_directory}/{gz_name}')
    except Exception as e:
        display('Extraction 4 fails')
        display(e)
    else:
        display('Done extracting data 4')

extract_data4(link4)


# ### Extract data source 5

# In[ ]:


link5 = 'https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/monthly/historical/derived_germany_soil_monthly_historical_stations_list.txt'

def extract_data5(link):
    try:
        save_position = data5_path
        display('Extracting data source 5')
        download(link,save_position)
    except Exception as e:
        display('Extraction 5 fails')
        display(e)
    else:
        display('Done Extracting data source 5')

extract_data5(link5)


# ### Prototype defining

# In[159]:


data1_prototype = {
    'year':'int64','co2':'float64','co2_growth_prct':'float64'
}

data2_prototype = {
    'year':'int64','month':'int64','Brandenburg/Berlin':'float64','Brandenburg':'float64','Baden-Wuerttemberg':'float64',
    'Bayern':'float64','Hessen':'float64','Mecklenburg-Vorpommern':'float64','Niedersachsen':'float64','Niedersachsen/Hamburg/Bremen':'float64',
    'Nordrhein-Westfalen':'float64','Rheinland-Pfalz':'float64','Schleswig-Holstein':'float64','Saarland':'float64',
    'Sachsen':'float64','Sachsen-Anhalt':'float64','Thueringen/Sachsen-Anhalt':'float64','Thueringen':'float64','Deutschland':'float64'
}

data3_prototype = {
    'year':'int64','month':'int64','Brandenburg/Berlin':'float64','Brandenburg':'float64','Baden-Wuerttemberg':'float64',
    'Bayern':'float64','Hessen':'float64','Mecklenburg-Vorpommern':'float64','Niedersachsen':'float64','Niedersachsen/Hamburg/Bremen':'float64',
    'Nordrhein-Westfalen':'float64','Rheinland-Pfalz':'float64','Schleswig-Holstein':'float64','Saarland':'float64',
    'Sachsen':'float64','Sachsen-Anhalt':'float64','Thueringen/Sachsen-Anhalt':'float64','Thueringen':'float64','Deutschland':'float64'
}

data4_prototype = {
    
    'Stationsindex':'int64','year':'int64','month':'int64','Mittel von TS05':'float64','Mittel von TS10':'float64','Mittel von TS20':'float64',
    'Mittel von TS50':'float64','Mittel von TS100':'float64','Mittel von TSLS05':'float64','Mittel von TSSL05':'float64','Maximum von ZFUMI':'float64',
    'Maximum von ZTKMI':'float64','Maximum von ZTUMI':'float64','Mittel von BFGL01_AG':'float64','Mittel von BFGL02_AG':'float64','Mittel von BFGL03_AG':'float64',
    'Mittel von BFGL04_AG':'float64','Mittel von BFGL05_AG':'float64','Mittel von BFGL06_AG':'float64','Mittel von BFGS_AG':'float64','Mittel von BFGL_AG':'float64',
    'Mittel von BFWS_AG':'float64','Mittel von BFMS_AG':'float64','Mittel von BFML_AG':'float64','Summe von VPGFAO':'float64','Summe von VPGH':'float64','Summe von VRGS_AG':'float64',
    'Summe von VRGL_AG':'float64','Summe von VRWS_AG':'float64','Summe von VRWL_AG':'float64','Summe von VRML_AG':'float64'
}


data5_prototype = {
    'Stationsindex':'int64',
    'Name':'string',
    'Bundesland':'string'
}


# ### Transform Data

# In[160]:


df1 = pd.read_csv(data1_path)
df2_list = read_all_csv_files(data2_directory,skiprows=1,delimiter=';')
df3_list = read_all_csv_files(data3_directory,skiprows=1,delimiter=';')
df4_list = read_all_csv_files(data4_directory,delimiter=';')
df5 = pd.read_csv(data5_path,delimiter=';',encoding='ISO 8859-15')


# In[161]:


from typing import Hashable

def German_to_English(string:str) -> str:

    string = string.replace('ä','ae')
    string = string.replace('ö','oe')
    string = string.replace('ü','ue')
    string = string.replace('ß','ss')

    return string


def merge_data_frames(data_frame_list) -> pd.DataFrame:
    return pd.concat(data_frame_list,axis=0)

def enforce_type(value,dtype):
    try:
        if dtype == 'int64':
            return int(value)
        elif dtype == 'float64':
            return float(value)
        elif dtype == 'string':
            return str(value)
        else:
            return value
    except (ValueError, TypeError):
        return None

def typing_filter(df,column_dtypes:Hashable):

    df = df.copy()

    for column,dtype in column_dtypes.items():
        df[column] = df[column].apply(lambda x: enforce_type(x,dtype))
    
    df.dropna(inplace=True)
    return df

def check_prototype(data_prototype,data_frame):

    columns = set(data_frame.columns)
    data_fields = set(data_prototype.keys())
    is_fit =  data_fields.issubset(columns)

    if not is_fit:
        raise TypeError(f'Expected columns:{data_fields} but actually:{columns}, difference:{data_fields - columns}')


# In[162]:


def transform_data1(df):

    # select the world data
    df = df[df['country'] == 'World']
    
    # select the row whose co2 is not null
    # and select [year,co2,co2_growth_prct] from it
    df = df[df['co2'].notna()]
    df = df[['year','co2','co2_growth_prct']]
    
    df = df.sort_values(by=['year'])
    
    #set the first column of co2_growth_prct to 0
    df.iloc[0,2] = 0

    #check prototype
    check_prototype(data1_prototype,df)

    # set the typing and filter out those not fitting
    df = typing_filter(df,data1_prototype)

    return df

def transform_data2(df_list):
    df = merge_data_frames(df_list)
    
    #drop the last error colum
    df.drop(df.columns[-1],axis=1,inplace=True)

    #rename the column name
    df.rename(columns={'Jahr':'year','Monat':'month'},inplace=True)

    #drop null rows
    df.dropna(inplace=True)

    # check prototype
    check_prototype(data2_prototype,df)

    # set the typing and filter out those not fitting
    df = typing_filter(df,data2_prototype)

    return df

def transform_data3(df_list):
    df = merge_data_frames(df_list)

    #drop the last error colum
    df.drop(df.columns[-1], axis=1, inplace=True)

    #rename the column name
    df.rename(columns={'Jahr':'year','Monat':'month'},inplace=True)

    #drop null rows
    df.dropna(inplace=True)

    # check prototype
    check_prototype(data3_prototype,df)

    # set the typing and filter out those not fitting
    df = typing_filter(df,data3_prototype)

    return df


def transform_data4(df_list):
    df = merge_data_frames(df_list)

    #drop the last error column
    df.drop(df.columns[-2:], axis=1, inplace=True)
    
    #drop any empty row
    df.dropna(inplace=True)



    # get time for year and month
    df['Jahr'] = df['Monat'] // 100
    df['Monat'] = df['Monat'] % 100

    #rename the column name
    df.rename(columns={'Jahr':'year','Monat':'month'},inplace=True)

    # check prototype
    check_prototype(data4_prototype,df)

    # set the typing and filter out those not fitting
    df = typing_filter(df,data4_prototype)

    return df
def transform_data5(df):

    #strip the column name
    df.columns = df.columns.str.strip()
    
    #select only the first,
    df =df[['Stationsindex','Name','Bundesland']]

    # for name column, remove the space
    df['Name'] = df['Name'].apply(lambda x: x.strip())

    #for bundesland colum, remove the space and convert German letter to English letter
    df['Bundesland'] = df['Bundesland'].apply(lambda x: German_to_English(x.strip()))

    #drop any empty row
    df = df.dropna()

    # check prototype
    check_prototype(data5_prototype,df)

    # set the typing and filter out those not fitting
    df = typing_filter(df,data5_prototype)

    return df


df1 = transform_data1(df1)
df2 = transform_data2(df2_list)
df3 = transform_data3(df3_list)
df4 = transform_data4(df4_list)
df5 = transform_data5(df5)


# In[163]:


def join_by_stationidx(data_frame4,data_frame5):
    data_frame45 = data_frame4.join(data_frame5.set_index('Stationsindex'),how='inner',on='Stationsindex')

    return data_frame45

# find their common year and month set
def find_common_year_month(data_frame2,data_frame3,data_frame45):
    
    year_month_set2 = set(data_frame2[['year','month']].itertuples(index=False, name=None))
    year_month_set3 = set(data_frame3[['year','month']].itertuples(index=False, name=None))
    year_month_set45 = set(data_frame45[['year','month']].itertuples(index=False, name=None))

    return year_month_set2 & year_month_set3 & year_month_set45

def filter_common_year_month(data_frame2,data_frame3,data_frame45):
    
    common_year_month = find_common_year_month(data_frame2,data_frame3,data_frame45)

    mask2 = data_frame2.apply(lambda row: (row['year'],row['month']) in common_year_month,axis=1)
    mask3 = data_frame3.apply(lambda row: (row['year'],row['month']) in common_year_month,axis=1)
    mask45 = data_frame45.apply(lambda row: (row['year'],row['month']) in common_year_month,axis=1)

    return data_frame2[mask2],data_frame3[mask3],data_frame45[mask45]


df45 = join_by_stationidx(df4,df5)
df2,df3,df45 = filter_common_year_month(df2,df3,df45)


# ### Load the data

# In[164]:


import sqlite3

db_file = '../data/data.sql'

if os.path.exists(db_file):
    # Delete the file
    os.remove(db_file)



conn = sqlite3.connect(db_file)


df1.set_index('year').to_sql('co2',if_exists='replace',index=True,con=conn)
df2.set_index(['year','month']).to_sql('temperature',if_exists='replace',index=True,con=conn)
df3.set_index(['year','month']).to_sql('precipitation',if_exists='replace',index=True,con=conn)
df45.set_index(['Stationsindex','year','month']).to_sql('soil',if_exists='replace',index=True,con=conn)


conn.close()

