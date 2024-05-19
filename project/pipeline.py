#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import requests
from tqdm import tqdm
import pandas as pd
from bs4 import BeautifulSoup
import time

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


if not os.path.exists('../data/download'):
    os.makedirs('../data/download')


# ### Extract data source 1

# In[2]:


link1 = 'https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv'
download(link1,'../data/download/data1.csv')


# ### Data 1 transformation

# In[3]:


df1 = pd.read_csv('../data/download/data1.csv')
df1.info()
df1 = df1[df1['country'] == 'World']
df1 = df1[df1['co2'].notna()][['year','co2','co2_growth_prct']]
df1.iloc[0,2] = 0
df1


# ### Load data source 1

# In[4]:


df1.to_csv('../data/co2_emission.csv')


# ### Extract data source 2

# In[5]:


link2 = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/'
content = fetch_html(link2)

soup = BeautifulSoup(content,'html.parser')


# In[6]:


csv_links = [(a.text,f'{link2}{a["href"]}') for a in soup.find_all('a') if a.text != '../']
csv_links


# In[7]:


for name,link in csv_links:
    download(link,f'../data/download/{name}')


# ### Data 2 transform

# In[8]:


df2_list = []

for i,(name,_) in enumerate(csv_links):
    df2_list.append(pd.read_csv(f'../data/download/{name}',skiprows=1,delimiter=';'))
    df2_list[i].drop(df2_list[i].columns[-1], axis=1, inplace=True)

df2_list[4]


# In[9]:


df2 = pd.concat(df2_list, axis=0).sort_values(by=['Jahr','Monat'])
df2 = df2.dropna()
df2.info()
df2


# ### Load data 2

# In[10]:


df2.to_csv('../data/temperature.csv')


# ### Extract data source 3

# In[11]:


link3 = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/precipitation/'
content = fetch_html(link3)

soup = BeautifulSoup(content,'html.parser')


# In[12]:


csv_links = [(a.text,f'{link3}{a["href"]}') for a in soup.find_all('a') if a.text != '../']
csv_links


# In[13]:


for name,link in csv_links:
    download(link,f'../data/download/{name}')


# ### Data source 3 transform

# In[14]:


df3_list = []

for i,(name,_) in enumerate(csv_links):
    df3_list.append(pd.read_csv(f'../data/download/{name}',skiprows=1,delimiter=';'))
    df3_list[i].drop(df3_list[i].columns[-1], axis=1, inplace=True)

df3_list[4]


# In[15]:


df3 = pd.concat(df3_list, axis=0).sort_values(by=['Jahr','Monat'])
df3 = df3.dropna()
df3.info()
df3


# ### Load Data source 3

# In[16]:


df3.to_csv('../data/precipitation.csv')


# ### Extract Data source 4

# In[17]:


link4 = 'https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/monthly/historical'
content = fetch_html(link4)

soup = BeautifulSoup(content,'html.parser')

files = [a['href'] for a in soup.find_all('a') if a['href'].endswith('.gz') and 'v2' in a['href']]
files


# In[18]:


for file in tqdm(files):
    time.sleep(1)
    download(f'{link4}/{file}',f'../data/download/{file}')


# ### Data source 4 transform

# In[19]:


df4_list = []

for file in tqdm(files):
    df = pd.read_csv(f'../data/download/{file}',delimiter=';')
    df.drop(df.columns[-2:], axis=1, inplace=True)
    df4_list.append(df)

df4_list[0]


# In[20]:


df4 = pd.concat(df4_list).dropna()
df4['Jahr'] = df4['Monat'] // 100
df4['Monat'] = df4['Monat'] % 100
df4 = df4.sort_values(by=['Stationsindex','Jahr','Monat'])
df4.info()
df4


# ### Load Data source 4

# In[21]:


df4.to_csv('../data/soil_condition.csv')


# #### Extract data source 5

# In[22]:


link5 = 'https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/monthly/historical/derived_germany_soil_monthly_historical_stations_list.txt'
download(link5,'../data/download/soil_stations_list.csv')


# ### Data source 5 transform

# In[23]:


df5 = pd.read_csv('../data/download/soil_stations_list.csv',delimiter=';',encoding='ISO 8859-15')
df5.info()
df5


# In[24]:


df5 =df5.iloc[:, [0,4,5]]
df5.iloc[:, 1] = df5.iloc[:, 1].apply(lambda x: x.strip())
df5.iloc[:, 2] = df5.iloc[:, 2].apply(lambda x: x.strip())
df5 = df5.dropna()
df5.info()
df5


# ### Load data 5

# In[25]:


df5.to_csv('../data/station.csv',encoding='UTF-8')

