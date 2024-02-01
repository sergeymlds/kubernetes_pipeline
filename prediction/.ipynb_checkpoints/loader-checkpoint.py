# -*- coding: utf-8 -*-
import boto3
import io
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from catboost import CatBoostRegressor
from concurrent.futures import ThreadPoolExecutor, as_completed
import json


# int
date_max = 0
container_name = 'spardata'

# ==================== yandex config ====================
with open('passwords.json', 'r') as j:
    pswrd = json.loads(j.read())

ya_service_name = 's3'
ya_url = 'https://storage.yandexcloud.net'
ya_key_id = pswrd['key_id']
ya_key = pswrd['key']
container_name = 'spardata'

session = boto3.session.Session()
s3 = session.client(
    service_name=ya_service_name,
    endpoint_url=ya_url,
    aws_access_key_id = ya_key_id,
    aws_secret_access_key = ya_key
    )


# ==================== functions ==================== 

def reader_excel(container_name, key_name):
    '''
        Get data from storage for a single operation:
          container_name -- str, container name in storage
          key_name -- str, way to data in storage
        Return: DataFrame
    '''
    s3 = session.client(
        service_name=ya_service_name,
        endpoint_url=ya_url,
        aws_access_key_id = ya_key_id,
        aws_secret_access_key = ya_key
    )
    response = s3.get_object(Bucket=container_name, 
                             Key=key_name)
    df = pd.read_excel(io.BytesIO(response['Body'].read()))
    return df


def reader_csv(container_name, key_name, is_header=True):
    '''
        Get data from storage for a single operation:
          container_name -- str, container name in storage
          key_name -- str, way to data in storage
          is_header -- bool, if TRUE header in data exist, FALSE header=None
        Return: DataFrame
    '''
    s3 = session.client(
        service_name=ya_service_name,
        endpoint_url=ya_url,
        aws_access_key_id = ya_key_id,
        aws_secret_access_key = ya_key
    )
    response = s3.get_object(Bucket=container_name, 
                             Key=key_name)
    if is_header:
        df = pd.read_csv(response.get("Body"))
    else:
        df = pd.read_csv(response.get("Body"), header=None)
    return df


def list_objects_in_container(container_name, prefix):
    '''
        Get all object names in bucket with need prefix:
            container_name -- str, bucket name
            prefix -- str, prefix name
        Return: List
    '''
    s3 = session.client(
        service_name=ya_service_name,
        endpoint_url=ya_url,
        aws_access_key_id = ya_key_id,
        aws_secret_access_key = ya_key
    )
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=container_name, Prefix=prefix)
    files = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                files += [obj['Key']]
    return files


def read_FilesbyPrfx_concat(container_name, prefix, is_header=True, concurr=True):
    '''
        Reading all files from the container/buket by a specific prefix
            container_name -- str, container name in storage
            prefix -- str, prefix -- str, prefix name
            is_header -- bool, if TRUE header in data exist, FALSE header=None 
            concurr -- bool, TRUE - use conThreadPoolExecutor
        Return: pd.DataFrame
    '''
    # get all need ibject ways by prefix
    obj_list = list_objects_in_container(container_name, prefix)
    # construct reader
    s3_task = lambda x: reader_csv(container_name=container_name, key_name=x, is_header=is_header)
    # if use concurrent reading 
    data = []
    if concurr:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(s3_task, f) for f in obj_list]
            for s3_thread in as_completed(futures):
                data += [s3_thread.result()]
    # is use standart reading 
    else:
        data = [s3_task(f) for f in obj_list]

    data = pd.concat(data)
    return data


def put_dfto_cloud(data, container_name, key_name):
    '''
        Uploading data to the cloud
            data -- pd.DataFrame, data to be saved
            container_name -- str, buket name
            key_name -- str, file way and name 
    '''
    s3 = session.client(
        service_name=ya_service_name,
        endpoint_url=ya_url,
        aws_access_key_id = ya_key_id,
        aws_secret_access_key = ya_key
    )
    with io.StringIO() as csv_buffer:
        data.to_csv(csv_buffer, index=False)
        response = s3.put_object(
            Bucket=container_name,
            Key=key_name,
            Body=csv_buffer.getvalue()
        )


def delete_s3_object(container_name, key_name):
    '''
        Remove all object names in bucket:
            container_name -- str, bucket name
            key_name -- str or list, obj(s) name(s)
        Return: pd.DataFrame
    '''
    s3 = session.client(
        service_name=ya_service_name,
        endpoint_url=ya_url,
        aws_access_key_id = ya_key_id,
        aws_secret_access_key = ya_key
    )   
    if isinstance(key_name, list):
        for k in key_name:
            s3.delete_object(Bucket=container_name, Key=k)
    elif isinstance(key_name, str):
        s3.delete_object(Bucket=container_name, Key=key_name)


def drange(x): 
    # make regular time grid in timeseries data
    global date_max
    e = date_max
    s = x.min()
    return pd.Series(pd.date_range(s,e))


def parse_provider(store_id, verbose=True):
    '''
        Overwriting information in Provider blobs from the database to the actual tables and to the archive:
            store_id -- str or int
            verbose -- bool, see or not information about unloading
    '''
     
    container_name = 'checks' # data / sales 
    prefix = f'provider/{store_id}/'

    # new files
    blob_names = list_objects_in_container(container_name, prefix)

    # check is exist actual df by this store_id
    actual_blob_name = list_objects_in_container(container_name, prefix=f'provider_actual/{store_id}/')

    if len(actual_blob_name) == 0:
        # if not create empty df
        df = pd.DataFrame()
    else:
        # if yes so df is actual df by this store_id
        df = reader_csv(container_name, f'provider_actual/{store_id}/provider.csv')
        df['time'] = pd.to_datetime(df['time'])

    # check is blob_names empty
    if len(blob_names) == 0:
        # do nothing 
        if verbose:
            print(f'blob_names is empty. If df is empty so your store_id {store_id} do not exist')
            pass
    else:
        # ================= start parsing ========================
        frames = []
        for blob in blob_names:
            time = datetime.strptime(blob.split('/')[3][:-4], '%Y-%m-%d %H:%M:%S.%f')
            operation = blob.split('/')[2]

            # get data from service by current blob
            cur_df = reader_csv(container_name, blob)
            cur_df['time'] = [time] * cur_df.shape[0]
            cur_df['operation'] = [operation] * cur_df.shape[0]
            frames.append(cur_df)

            # dump all used blobs to archive
            put_dfto_cloud(data=cur_df, container_name=container_name, key_name=f"archive/{blob}")

        updates = pd.concat(frames)
        df = pd.concat([df, updates], ignore_index=True)

        # sort by datetime
        df = df.sort_values(by='time').reset_index(drop=True)

        # keep only last insert
        df = df.drop_duplicates(subset=['Item', 'operation'], keep='last').reset_index(drop=True)
        
        df = df.groupby('Item', as_index=False).last()
        df = df[df['operation'] == 'insert']
        put_dfto_cloud(data=df, container_name=container_name, key_name=f'provider_actual/{store_id}/provider.csv')

        if verbose:
            print(f'success update "provider_actual/{store_id}/provider.csv"!')
        
        # delete all temp provider files
        delete_s3_object(container_name=container_name, key_name=blob_names)
        if verbose:
            print(f'delete all temp provider files "provider/{store_id}/..."!')
        
    return df


def load_weather_history(container_name, city):
    # read weather history by city
    weather = read_FilesbyPrfx_concat(container_name, f'weather_history/{city}', is_header=True)
    weather['date'] = pd.to_datetime(weather['date'])
    return weather


def load_sales(container_name, store_id):
    
    global date_max
    sales = read_FilesbyPrfx_concat(container_name, f'check/{store_id}', is_header=True)
    sales = sales.rename(columns = {'DateFact':'date', 'Item':'item_id',
                                    'PriceBase':'price_base', 'Qnty':'qnty'})

    # read markdowns
    try:
        markdowns = read_FilesbyPrfx_concat(container_name, f'markdown/{store_id}', is_header=False)
        markdowns.columns = ["date", "Item", "NormalPrice", "Price", "Qnty"]
        # merge markdown and sales
        sales = sales.merge(markdowns, how='left', left_on=['date', 'item_id'], right_on=['date', 'Item'])

        sales['new_qnty'] = sales['qnty'] - sales['Qnty']            
        sales.loc[sales['new_qnty'] < 0, 'new_qnty'] = 0
        sales.loc[~sales['new_qnty'].isna(), 'qnty'] = sales.loc[~sales['new_qnty'].isna(), 'new_qnty']

        sales = sales[['date', 'item_id', 'qnty', 'price_base', 'SumTotal']]
    except:
        sales = sales[['date', 'item_id', 'qnty', 'price_base', 'SumTotal']]
        
    sales['date'] = pd.to_datetime(sales['date'])
    sales['item_id'] = sales['item_id'].astype(int)
    sales['price_base'] = sales['price_base'].astype(float)
    sales['qnty'] = sales['qnty'].astype(float)

    ######################
    ######################

    start_obs = reader_csv(container_name, r'actual/{}/actual1.csv'.format(store_id))

    start_obs = start_obs.rename(columns = {'Item':'item_id'})
    start_obs = start_obs[['item_id', 'date']]
    start_obs['date'] = pd.to_datetime(start_obs['date'])

    date_max = sales['date'].max()
    start_obs = start_obs.groupby('item_id').date.apply(drange).reset_index(level=0)
    sales = sales.merge(start_obs, how = 'outer')
    sales['qnty'] = sales['qnty'].fillna(0)
    
    # read online
    try:
        online = read_FilesbyPrfx_concat(container_name, f'online/{store_id}', is_header=True)
        online = online[['DateFact', 'Item', 'Qnty']]
        online = online.rename(columns = {'DateFact':'date', 'Item':'item_id', 'Qnty':'new_qnty'})
        online['date'] = pd.to_datetime(online['date'])
        online = online[online['new_qnty'] > 0]
        
        sales = sales.merge(online, on = ['date', 'item_id'], how = 'left')
        
        sales['new_qnty'] = sales['new_qnty'].fillna(0)
        sales['qnty'] = sales['qnty'] + sales['new_qnty']
                
        sales = sales[['date', 'item_id', 'qnty', 'price_base', 'SumTotal']]
    except Exception as e:
        print(e)
        sales = sales[['date', 'item_id', 'qnty', 'price_base', 'SumTotal']]
    
    # price_history
    try:
        price_history = read_FilesbyPrfx_concat(container_name, f'price_history/{store_id}', is_header=False)
        price_history.columns = ['date', 'item_id', 'price', 'code']
        price_history = price_history.drop_duplicates(['date', 'item_id'], keep='last')

        price_history['date'] = pd.to_datetime(price_history['date'])
        sales = sales.sort_values(['item_id', 'date'])
        sales = sales.merge(price_history, how='left', on=['date', 'item_id'])

        sales['code'] = sales.groupby(['item_id'])['code'].transform(lambda x: x.fillna(method = 'ffill', limit=7))
        sales_to_delete = sales[(sales['code'] == 3)].index
        sales.drop(sales_to_delete , inplace=True)
        
    except Exception as e:
        print(e)
    return sales


def load_stock(store_id):
    # read stock data from blob
    stock = read_FilesbyPrfx_concat(container_name, f'stock/{store_id}', is_header=True)
    stock = stock.rename(columns = {'DateEnd':'date', 'Item':'item_id'})
    stock = stock[['date', 'item_id', 'StockQuantity']]
    stock['date'] = pd.to_datetime(stock['date'])
    stock['item_id'] = stock['item_id'].astype(int)
    stock['StockQuantity'] = stock['StockQuantity'].astype(float)

    #selecting last half year for feature extraction
    #stock = stock[stock['date'] > datetime.now() - timedelta(180)]
    #sales = sales[sales['date'] > datetime.now() - timedelta(180)]
    return stock


def load_holidays(store_id):
    holidays = reader_csv(container_name, f'holidays/{store_id}/holidays.csv')
    holidays = holidays.drop_duplicates(subset = 'date')

    holidays['date'] = pd.to_datetime(holidays['date'])
    holidays['month'] = holidays['month'].astype('uint8')
    holidays['year'] = holidays['year'].astype('uint16')
    holidays['day'] = holidays['day'].astype('uint8')
    holidays['holiday'] = holidays['holiday'].astype(bool)
    holidays['before_holiday'] = holidays['before_holiday'].astype(bool)
    holidays['weekday'] = holidays['weekday'].astype('uint8')
    holidays['mart'] = holidays['mart'].astype(bool)
    holidays['apr'] = holidays['apr'].astype(bool)
    holidays['dec'] = holidays['dec'].astype(bool)
    holidays['nov'] = holidays['nov'].astype(bool)
    holidays['jun'] = holidays['jun'].astype(bool)
    holidays['may'] = holidays['may'].astype(bool)
    holidays['jan'] = holidays['jan'].astype(bool)
    return holidays


def load_discounts(store_id, price_base):
    
    discounts = reader_csv(container_name, f'disc_history1/{store_id}/discount.csv')
    discounts = discounts.rename(columns = {'Item':'item_id'})
    discounts = discounts.drop('doc_id', axis = 1)
    discounts = discounts[discounts['PromoTypeCode']!=28]
    
    def to_int(x):
        try:
            return int(x)
        except:
            return -1

    try:
        discounts['item_id'] = discounts['item_id'].astype(int)
    except:
        discounts['item_id'] = discounts['item_id'].map(lambda x: to_int(x))
        discounts = discounts[discounts['item_id'] > 0]

    discounts = discounts.drop_duplicates(subset = ['date', 'item_id'])
    discounts = discounts.merge(price_base, how = 'left')
    discounts.loc[discounts['SalePriceBeforePromo'] == 0, 
                  'SalePriceBeforePromo'] = discounts.loc[discounts['SalePriceBeforePromo'] == 0, 'price_base']
    discounts['PromoTypeCode'] = discounts['PromoTypeCode'].fillna(0).astype('uint8')

    try:
        discounts['SalePriceBeforePromo'] = discounts['SalePriceBeforePromo'].astype(float)
    except:
        print('can not transform  SalePriceBeforePromo type')

    try:
        discounts['SalePriceTimePromo'] = discounts['SalePriceTimePromo'].astype(float)
    except:
        print('can not transform  SalePriceTimePromo type')

    discounts['discount'] = (discounts['SalePriceBeforePromo'] - discounts['SalePriceTimePromo'])/discounts['SalePriceBeforePromo']
    discounts['discount'] = discounts['discount'].map(lambda x: 0.2 if x < 0 else x)
    discounts['discount'] = discounts['discount'].map(lambda x: round(x, 2))
    discounts = discounts[['date', 'item_id', 'PromoTypeCode', 'number_disc_day', 'discount']]
    discounts['date'] = pd.to_datetime(discounts['date'])
    return discounts


def load_categories(store_id):
    categories = reader_csv(container_name, r'actual/{}/actual1.csv'.format(store_id))
    categories = categories.rename(columns = {'Item':'item_id'})
    categories.date = pd.to_datetime(categories.date)
    return categories


def load_model(store_id):
    s3 = session.client(
        service_name=ya_service_name,
        endpoint_url=ya_url,
        aws_access_key_id = ya_key_id,
        aws_secret_access_key = ya_key
    )
    # загружаем модель в поток
    m_file = s3.get_object(Bucket='sparmodels', 
                           Key=f'{store_id}/model')
    # создаем модель
    model = CatBoostRegressor()
    model.load_model(stream=m_file['Body'])
    return model


# def load_consumption(consumption_dir, store_id):

#     catalog = reader_csv('spardata', 'catalog/{}/catalog.csv'.format(store_id))
#     #read consumption data from blob
#     consumption_list = os.listdir(consumption_dir + '/' + str(store_id))
#     consumption_paths = [consumption_dir + '/' +  str(store_id) + '/' + file_name for file_name in consumption_list]
#     consumption = pd.concat([pd.read_csv(f) for f in consumption_paths])
#     #print(consumption.head())
#     consumption = consumption.rename(columns = {'DateFact':'date', 'Item':'item_id', 'Qnty':'qnty'})
#     consumption = consumption[['date', 'item_id', 'qnty', 'Type', 'CodeOperation']]
#     consumption['date'] = pd.to_datetime(consumption['date'])
#     consumption['item_id'] = consumption['item_id'].astype(int)
#     consumption['qnty'] = consumption['qnty'].astype(float)

#     global date_max
#     date_max = consumption['date'].max()

#     production = consumption[consumption['CodeOperation'] == 1]
#     #production.head(2)

#     # prod - то, что участвует в производстве
#     prod  = catalog[catalog['Type'] == 1]
#     start_obs = consumption.groupby('item_id').date.apply(drange).reset_index(level=0)

#     production = production[production['item_id'].isin(prod['EntityId'])]

#     # разделяем sku и ингредиент
#     production_sku = production[production['Type'] == 1]
#     production_ingredient = production[production['Type'] == 2]

#     # подтягиваем СКЮ
#     production_ingredient = production_ingredient.merge(prod.drop('Type', axis = 1), 
#                                                  left_on = 'item_id', right_on = 'EntityId', how = 'left')
#     production_ingredient['item_id'] = production_ingredient['Item']

#     # умножаем на коэффициент замены
#     production_ingredient['qnty'] = production_ingredient['qnty']*production_ingredient['ReplacementCoeff']
#     production_ingredient = production_ingredient.drop(['EntityId', 'Item', 'ReplacementCoeff'], axis = 1)
#     production_ingredient  = pd.concat([production_ingredient, production_sku])
#     production_ingredient = production_ingredient.drop(['Type', 'CodeOperation'], axis = 1)
#     production_ingredient = production_ingredient.groupby(['date', 'item_id'], as_index = False)['qnty'].sum()


#     #создание датафрейма с текущими датами, для расчета расхода с последней поставки
#     last_date = pd.to_datetime(datetime.now().date())
#     production_ingredient_now = pd.DataFrame()
#     production_ingredient_now['item_id'] = list(set(production_ingredient['item_id']))
#     production_ingredient_now['date'] = last_date
#     production_ingredient_now['qnty'] = 0

#     #пересчет на дневной расход
#     columns_order = production_ingredient.columns
#     production_ingredient = pd.concat([production_ingredient, production_ingredient_now[columns_order]])
#     consumption = production_ingredient.sort_values(['item_id', 'date'])

#     consumption['item_id1'] = consumption['item_id']
#     consumption['qnty1'] = consumption['qnty']
#     consumption['date1'] = consumption['date']
#     consumption[['date1', 'item_id1', 'qnty1']] = consumption[['date1', 'item_id1', 'qnty1']].shift(1)
#     consumption = consumption[consumption['item_id1'] == consumption['item_id']]

#     consumption['lag'] = (consumption['date'] - consumption['date1']).dt.days
#     consumption['qnty1'] = consumption['qnty1']/consumption['lag']


#     #фиктивная дата для более точного расчета актуального расхода
#     consumption_last = consumption[consumption['date'] == last_date]
#     # последний день
#     consumption_last = consumption[consumption['date'] == last_date]
#     consumption_last = consumption_last[consumption_last['lag'] != 0]

#     # расход перед последним днем
#     consumption_before_last = consumption[consumption['date'] < last_date]
#     before_last_max_date = consumption_before_last.groupby(['item_id'], as_index = False)['date'].max()
#     consumption_before_last = consumption_before_last.merge(before_last_max_date, how = 'inner')

#     consumption_before_last = consumption_before_last[['date', 'item_id', 'qnty1']].rename(columns = {'qnty1':'qnty1_before_last'})

#     consumption_last = consumption_last[['date', 'item_id', 'qnty1']].rename(columns = {'qnty1':'qnty1_last'})

#     exclude_last_date = consumption_before_last[['item_id', 'qnty1_before_last']].merge(consumption_last[['item_id', 'qnty1_last']])
#     exclude_last_date = exclude_last_date[exclude_last_date['qnty1_before_last'] < exclude_last_date['qnty1_last']]
#     exclude_last_date = set(exclude_last_date['item_id'])

#     # удаляем последнюю поставку, если необходимо
#     consumption = consumption[~((consumption['lag'] == 0)|
#                                 (consumption['item_id'].isin(exclude_last_date)&
#                                  (consumption['date'] == last_date)))]

#     result = consumption.groupby('item_id1').date1.apply(drange).reset_index(level=0)
#     result = result.merge(consumption, how = 'left')
#     result['qnty1'] = result.groupby('item_id1')['qnty1'].transform(lambda x: x.fillna(method = 'bfill').fillna(method = 'ffill'))

#     result = result[['date1', 'item_id1', 'qnty1']]
#     result = result.rename(columns = {'date1':'date', 'item_id1':'item_id', 'qnty1':'qnty'})

#     return result

def load_consumption(store_id):
    # read consumption data from blob  
    consumption = read_FilesbyPrfx_concat(container_name, f'consumption/{store_id}', is_header=True)
    print(consumption.head())
    consumption = consumption.rename(columns = {'DateFact':'date', 'Item':'item_id', 'Qnty':'qnty'})
    consumption = consumption[['date', 'item_id', 'qnty', 'Type', 'CodeOperation']]
    consumption = consumption[~(consumption['qnty'] > 50000)]
    consumption['date'] = pd.to_datetime(consumption['date'])
    consumption['item_id'] = consumption['item_id'].astype(int)
    consumption['qnty'] = consumption['qnty'].astype(float)
    return consumption


def production_ing_to_sku(production, catalog):
    global date_max
    date_max = production['date'].max()

    # prod - то, что участвует в производстве
    prod  = catalog[catalog['Type'] == 1]
    start_obs = production.groupby('item_id').date.apply(drange).reset_index(level=0)


    # разделяем sku и ингредиент
    production_ingredient = production[production['Type'] == 2]
    production_sku = production[production['Type'] == 1]

    # подтягиваем СКЮ
    production_ingredient = production_ingredient.merge(prod.drop('Type', axis = 1), left_on = 'item_id', right_on = 'EntityId', how = 'left')
    production_ingredient['item_id'] = production_ingredient['Item']

    # умножаем на коэффициент замены
    production_ingredient['qnty'] = production_ingredient['qnty']*production_ingredient['ReplacementCoeff']
    production_ingredient = production_ingredient.drop(['EntityId', 'Item', 'ReplacementCoeff'], axis = 1)
    production  = pd.concat([production_ingredient, production_sku])
    
    production = production.drop(['Type', 'CodeOperation'], axis = 1)
    #production = pd.concat([write_off[['date', 'item_id', 'qnty']], production])
    production = production.groupby(['date', 'item_id'], as_index = False)['qnty'].sum()
    
    return production


def consumption_rate(production):
  
    production = production[['date', 'item_id', 'qnty']]
    #создание датафрейма с текущими датами, для расчета расхода с последней поставки
    last_date = pd.to_datetime(datetime.now().date())
    production_now = pd.DataFrame()
    production_now['item_id'] = list(set(production['item_id']))
    production_now['date'] = last_date
    production_now['qnty'] = 0
    
    #пересчет на дневной расход
    columns_order = production.columns
    print(columns_order)
    production = pd.concat([production, production_now[columns_order]])
    production = production.sort_values(['item_id', 'date'])

    production['item_id1'] = production['item_id']
    production['qnty1'] = production['qnty']
    production['date1'] = production['date']
    production[['date1', 'item_id1', 'qnty1']] = production[['date1', 'item_id1', 'qnty1']].shift(1)
    production = production[production['item_id1'] == production['item_id']]

    production['lag'] = (production['date'] - production['date1']).dt.days
    production['qnty1'] = production['qnty1'] / production['lag']
    
    # фиктивная дата для более точного расчета актуального расхода
    production_last = production[production['date'] == last_date]
    # последний день
    production_last = production[production['date'] == last_date]
    production_last = production_last[production_last['lag'] != 0]

    # расход перед последним днем
    production_before_last = production[production['date'] < last_date]
    before_last_max_date = production_before_last.groupby(['item_id'], as_index = False)['date'].max()
    production_before_last = production_before_last.merge(before_last_max_date, how = 'inner')

    production_before_last = production_before_last[['date', 'item_id', 'qnty1']].rename(columns = {'qnty1':'qnty1_before_last'})

    production_last = production_last[['date', 'item_id', 'qnty1']].rename(columns = {'qnty1':'qnty1_last'})

    exclude_last_date = production_before_last[['item_id', 'qnty1_before_last']].merge(production_last[['item_id', 'qnty1_last']])
    exclude_last_date = exclude_last_date[exclude_last_date['qnty1_before_last'] < exclude_last_date['qnty1_last']]
    exclude_last_date = set(exclude_last_date['item_id'])

    # удаляем последнюю поставку, если необходимо
    production = production[~((production['lag'] == 0)|
                                (production['item_id'].isin(exclude_last_date)&
                                 (production['date'] == last_date)))]
    
    result = production.groupby('item_id1').date1.apply(drange).reset_index(level=0)
    result['date1'] = pd.to_datetime(result['date1'])
    result = result.merge(production, how = 'left')
    result['qnty1'] = result.groupby('item_id1')['qnty1'].transform(lambda x: x.fillna(method = 'ffill').fillna(method = 'bfill'))

    result = result[['date1', 'item_id1', 'qnty1']]
    result = result.rename(columns = {'date1':'date', 'item_id1':'item_id', 'qnty1':'qnty'})
    
    return result

# def write_off_coefficient(write_off_dir, write_off, store_id):
#     write_off_list = os.listdir(write_off_dir + '/' + str(store_id))
#     write_off_paths = [write_off_dir + '/' +  str(store_id) + '/' + file_name for file_name in write_off_list]

#     print(write_off_paths)

#     if len(write_off_paths)>1:
#         for i in range(0, len(write_off_paths)-1):
#             if pd.to_datetime(write_off_paths[i].split('/')[-1].split('.')[0]) >= (datetime.now() - timedelta(days=21)):
#                 write_off_history = pd.read_csv(write_off_paths[i]).merge(pd.read_csv(write_off_paths[i+1]).rename(columns={'coef':'coef_new'}))
#                 write_off_history['coef_changes'] = write_off_history['coef_new']/write_off_history['coef']
#                 write_off_history = write_off_history[['item_id', 'coef_changes']]

#                 changes_date = write_off_paths[i+1].split('/')[-1].split('.')[0]

#                 write_off = write_off.merge(write_off_history, how='left')
#                 write_off['coef_changes'] = write_off['coef_changes'].fillna(1)

#                 write_off.loc[(write_off['date']<=changes_date)&(write_off['coef_changes']!=1), 'qnty'] = write_off.loc[(write_off['date']<=changes_date)&(write_off['coef_changes']!=1), 'qnty'] * write_off.loc[(write_off['date']<=changes_date)&(write_off['coef_changes']!=1), 'coef_changes']
#                 write_off = write_off.drop('coef_changes',axis=1)
#     #pd.read_csv(path)
#     return write_off


def write_off_coefficient(write_off, store_id):
    frames = []
    coef_changes = read_FilesbyPrfx_concat(container_name, f'write_off_history/{store_id}', is_header=True)
    #coef_changes = coef_changes.astype({'coef':'float'})
    coef_changes['date'] = pd.to_datetime(coef_changes['date'])
    
    changes_dates = sorted(list(set(coef_changes['date'])))
    
    today = datetime.now()

    if len(coef_changes[coef_changes['date'] > today - timedelta(21)]) > 0:
        df_21day = coef_changes[coef_changes['date'] >= today - timedelta(21)]
        df_old = coef_changes[coef_changes['date'] < today - timedelta(21)]
        df_old = df_old[df_old['date'] == df_old['date'].max()]
        coef_changes = pd.concat([df_old, df_21day])
        
        coef_changes = coef_changes.pivot_table(index = 'item_id', columns = 'date', values = 'coef')
        data = coef_changes[coef_changes.columns[1:]].values/coef_changes[coef_changes.columns[:-1]].values
        coef_changes_columns = coef_changes.columns[1:]
        result = pd.DataFrame(data = data, columns = coef_changes_columns, index = coef_changes.index)
        result = result.replace(np.inf, 1).fillna(1)
        result = result.unstack().reset_index().rename(columns = {0:'coef_changes'})
        
        for changes_date in changes_dates:
            result1 = result[result['date'] == changes_date][['item_id', 'coef_changes']]
            write_off = write_off.merge(result1, how = 'left').fillna(1)
            
            mask_cond = (write_off['date']<=changes_date) & (write_off['coef_changes']!=1)
            write_off.loc[mask_cond, 'qnty'] = write_off.loc[mask_cond, 'qnty'] * write_off.loc[mask_cond, 'coef_changes']

            write_off = write_off.drop('coef_changes',axis=1)
        
    return write_off
