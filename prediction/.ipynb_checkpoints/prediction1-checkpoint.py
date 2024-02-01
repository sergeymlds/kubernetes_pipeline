# -*- coding: utf-8 -*-
# ==================== imports ==================== 
import boto3
import os
import time
import io
import argparse
import pandas as pd
import time
import numpy as np
from catboost import CatBoostRegressor
from functools import reduce
from datetime import datetime, timedelta
from dateutil.easter import *
import json
import traceback
import sys
import requests

#модуль с загрузками файлов
from loader import (load_sales, load_stock, load_holidays, load_discounts, drange, load_categories, 
                    load_model, reader_csv, load_consumption, production_ing_to_sku, consumption_rate, 
                    write_off_coefficient, put_dfto_cloud, parse_provider)
#модуль для поправок на праздники
from holiday_amendments import prior_year_sales_amendens
#модуль для корректировок данных
from features import (zero_forecasting_subclass, remove_anormal_days, get_max_and_median_sales, 
                      remove_price_history_from_office, get_rolling_median_and_mean_sales, type_conversion, 
                      get_weekday_median_features, get_weekday_features, get_dataset, get_x)
#модуль для корректировки прогнозов
from predict import (get_predict, get_prediction_from_month_plan, limiting_nondiscont_prediction, 
                     limiting_discont_prediction, limiting_lower_discont_prediction, the_lower_limit, 
                     season_coefficient)
#модуль для расчёта коэффициентов для товаров, у которых продажи зависят от погоды
from weather import get_weather_coeff

from discounts_onpromo import get_onpromo_prediction
# #модуль для корректировки прогноза из-за анломалий февраля 2022 года
# from fixing_pred import get_correct_prediction


# ============================ func ============================ 
def telegram_bot_sendtext(bot_message):

    bot_token = '437775212:AAFZWZxeJ5QxwMgTwjjtDvsajlOF3taHyqo'
    bot_chatID = '-238099160'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&text=' + bot_message

    response = requests.get(send_text)
    return response.json()


# ==================== yandex config ====================
with open('passwords.json', 'r') as j:
    pswrd = json.loads(j.read())

ya_service_name = 's3'
ya_url = 'https://storage.yandexcloud.net'
ya_key_id = pswrd['key_id']
ya_key = pswrd['key']
container_name = 'spardata'

session = boto3.session.Session()


# ==================== parser config ==================== 
parser = argparse.ArgumentParser(description="Start a tensorflow model serving")
parser.add_argument('--stores', dest="stores", required=True, nargs='+')
args = parser.parse_args()
stores = args.stores
stores = stores[0].split(',')

# ==================== main part ==================== 
exclude_items_complectatin = reader_csv('spardata', 'exclude_items_complectatin.csv')
exclude_items_complectatin = set(exclude_items_complectatin['item_id'])

remove_items_from_forecast = reader_csv('spardata', 'remove_items_from_forecast.csv')
remove_items_from_forecast = remove_items_from_forecast['item_id'].tolist()

for store_id in stores:
    try:
        # загрузка чеков (продажи)
        sales = load_sales(container_name, store_id)

        # запись продаж за последний месяц для телеграм бота
        output = sales[sales['date'] >= (datetime.now() - timedelta(days=31))][
            ['date', 'item_id', 'qnty', 'price_base', 'SumTotal']]
        put_dfto_cloud(output, 'planning', r'telegram/sales/{}.csv'.format(store_id))

        # запись производства в блоб для телеграм-бота
        categories = load_categories(store_id)
        cons_last_month = sales[sales['date']>=(datetime.now()-timedelta(days=31))]
        cons_last_month = cons_last_month.merge(categories[(categories['DIVISION'].isin([6412,6416])) & 
                                                           (~categories['DEPT'].isin([5770,5771,5772]))] 
                                         .drop(['date'], axis=1))[['date', 'item_id', 'qnty', 'price_base', 'SumTotal']]
        put_dfto_cloud(cons_last_month, 'planning', r'telegram/{}.csv'.format(store_id))

        # загрузка производства
        consumption = load_consumption(store_id)
        # загрузка каталога
        catalog = reader_csv('spardata', 'catalog/{}/catalog.csv'.format(store_id))

        # производство
        production = consumption[consumption['CodeOperation'] == 1]
        # функция переводит ингредиент в СКЮ
        production = production_ing_to_sku(production, catalog)
        # функция равномерно распределяет расход между датами, где были записи
        production = consumption_rate(production)

        # списание
        write_off = consumption[consumption['CodeOperation'] == 2]
        write_off = consumption_rate(write_off)
        try:
            write_off = write_off_coefficient(write_off, store_id)
        except:
            print('There are no write off coefficients')


        # комплектация
        complectation = consumption[(consumption['CodeOperation'] == 3)&(consumption['Type'] == 1)]
        # выбираем только те СКЮ, которые участвуют в комплектации 
        complect_sku_set = set(catalog[catalog['Type'] == 2]['Item'])
        complectation = complectation[complectation['item_id'].isin(complect_sku_set)]
        complectation = consumption_rate(complectation)
        # убираем вторичный учет расхода товара в комплектации 
        complectation = complectation[~complectation['item_id'].isin(exclude_items_complectatin)]

        # сбор в кучу 
        consumption = pd.concat([production, write_off, complectation])
        consumption = consumption.groupby(['date', 'item_id'], as_index = False)['qnty'].sum()

                # выбираем только товары производства
        #         production_sku = reader_csv('spardata', r'production_sku.csv')
        #         production_sku = set(production_sku['item_id'])
        #         consumption = consumption[consumption['item_id'].isin(production_sku)]

        # дополнение расходов производством 
        sales = sales.merge(consumption.rename(columns = {'qnty' : 'qnty1'}), how = 'left')
        sales['qnty'] = sales['qnty'] + sales['qnty1'].fillna(0)
        sales = sales.drop('qnty1', axis = 1)

        #выбор неакционных продаж товаров, у которых продажи зависят от погоды
        weather_groups = reader_csv('spardata', r'weather_groups.csv')
        categories = load_categories(store_id)
        categories = categories.merge(weather_groups[['SUBCLASS']])
        sales_weather = sales.merge(categories[['item_id']])

        #медианные цены на товары
        price_base = sales.groupby(['item_id'], as_index=False).agg({'price_base':'median'})

        #загрузка остатков
        stock = load_stock(store_id)

        #загрузка праздников
        holidays = load_holidays(1354)

        #загрузка акций
        discounts = load_discounts(store_id, price_base)

        #выбор неакционных продаж товаров, у которых продажи зависят от погоды
        items = set(sales_weather['item_id'])
        discounts_weather = discounts[discounts['item_id'].isin(items)]

        #загрузка погоды
        categories = load_categories(store_id)
        cities = reader_csv('spardata', r'cities.csv')

        weather_history = reader_csv(
            'spardata', r'weather_history/{}/weather.csv'.format(
                cities[cities['ObjCode']==int(store_id)]['City'].values[0])
        )
        weather_history['date'] = pd.to_datetime(weather_history['date'])

        weather_forecast = reader_csv(
            'spardata', r'weather_forecast/{}/weather.csv'.format(
                cities[cities['ObjCode']==int(store_id)]['City'].values[0])
        )
        weather_forecast['date'] = pd.to_datetime(weather_forecast['date'])

        #коэффициенты для классов товаров, у которых продажи зависят от погоды
        coef = get_weather_coeff(sales_weather, weather_history, discounts_weather, categories, weather_forecast)

        sales = sales.merge(discounts[['date', 'item_id', 'PromoTypeCode']], how = 'left')

        #почистить неразмеченные акции
        # sales_to_delete = sales[((sales['code']==9)|
        #                (sales['code']==18)|
        #                (sales['code']==19)|
        #                (sales['code']==29)|
        #                (sales['code']==31))&
        #               (sales['PromoTypeCode'].isna())].index

        # sales.drop(sales_to_delete , inplace=True)
        # sales = sales.drop(['price', 'code'], axis = 1)

        #ротация пива
        beer = reader_csv('spardata', r'alternative/beer.csv')
        beer_map = beer[beer['new_id'].isin(sales['item_id'])][['new_id', 'old_id']].set_index('old_id').to_dict()['new_id']
        sales = sales.replace({"item_id": beer_map})
        sales = sales.drop_duplicates(['date', 'item_id'], keep='last')

        #ротация бокалов
        bokaly = reader_csv('spardata', r'alternative/bokaly.csv')
        bokaly_map = bokaly[~bokaly['old_id'].isna()][['old_id', 'new_id']].set_index('old_id').to_dict()['new_id']
        sales = sales.replace({"item_id": bokaly_map})

        #взять последние полгода продаж
        sales = sales[(sales['date'] > datetime.now() - timedelta(180))|sales['PromoTypeCode'].notnull()]
        # sales = sales.drop('PromoTypeCode', axis = 1)

        #заполнить нулевыми продажами
        date_max = sales['date'].max()
        res = sales.groupby('item_id').date.apply(drange).reset_index(level=0)

        #получть категории товаров
        categories = load_categories(store_id)
        res = res.merge(categories[['item_id', 'DIVISION', 'GROUP_NO', 'DEPT', 'CLASS', 'SUBCLASS']])
        sales = res.merge(sales, how = 'left')

        #почистить неразмеченные акции
        sales_to_delete = sales[((sales['code']==9)|
                       (sales['code']==18)|
                       (sales['code']==19)|
                       (sales['code']==29)|
                       (sales['code']==31))&
                      (sales['PromoTypeCode'].isna())].index

        sales.drop(sales_to_delete , inplace=True)
        sales = sales.drop(['price', 'code', 'PromoTypeCode'], axis = 1)

        sales = sales.merge(stock, how = 'left').fillna(0)

        #получить дополнительную колонку остатков, которая сдвинута на день назад, чтобы товара хватало на понедельник
        stock_shift = sales[['date', 'item_id', 'StockQuantity']].copy()
        stock_shift['date'] = stock_shift['date'] - timedelta(1)
        stock_shift = stock_shift.rename(columns={'StockQuantity':'StockQuantity_shift'})
        sales = sales.merge(stock_shift, on=['date', 'item_id'], how='left')
        sales['StockQuantity_shift'] = sales['StockQuantity_shift'].fillna(1)

        sales.loc[sales['price_base']==0, 'price_base'] = np.nan
        sales['price_base'] = sales.groupby(['item_id'])['price_base'] \
                    .transform(lambda x: x.fillna(method = 'ffill').fillna(method = 'bfill'))
        sales['price_base'] = sales['price_base'].fillna(0)

        #увеличить продажи в 1.5р для товаров с отрицательными и нулевыми остатками
        subclasses_list = reader_csv('spardata', r'subclasses.csv')
        subclasses_list = subclasses_list['SUBCLASS'].to_list()
        sales.loc[(sales['StockQuantity_shift'] <= 0) & (sales['StockQuantity_shift'] >= -50) & (~sales['SUBCLASS'].isin(subclasses_list)) & (~sales['item_id'].isin(consumption['item_id'])), 'qnty'] = \
               1.5*sales.loc[(sales['StockQuantity_shift'] <= 0) & (sales['StockQuantity_shift'] >= -50) & (~sales['SUBCLASS'].isin(subclasses_list)) & (~sales['item_id'].isin(consumption['item_id'])), 'qnty']

        #убрать нулевые продажи для для товаров с отрицательными и нулевыми остатками, которые дешевле 500р
        sales = sales[~((sales['StockQuantity'] <= 0) & (sales['qnty'] == 0) & ((sales['price_base'] < 500) | (sales['CLASS']==5825)) )]
        sales.drop('StockQuantity', axis= 1, inplace = True)

        del stock
        del res

        sales = sales.fillna(0)[['date', 'item_id', 'qnty', 'DIVISION', 'GROUP_NO', 'DEPT', 'CLASS', 'SUBCLASS']]
        sales = sales.merge(price_base, how = 'left')
        sales = sales.merge(discounts, on = ['date', 'item_id'], how = 'left')

        #убрать аномальные продажи
        sales = remove_anormal_days(sales)

        #убрать уценки с кодом 3
        sales = remove_price_history_from_office(store_id, sales)

        #удалить предпасхальные продажи для ряда товаров
        if datetime.now() > pd.to_datetime(easter(datetime.now().year, EASTER_ORTHODOX)):
            sales_to_delete = sales[(sales['CLASS'].isin(['5863', '5860', '5877', '5252', '6780', '5256']))&
                            (sales['date']<=pd.to_datetime(easter(datetime.now().year, EASTER_ORTHODOX)))&
                            (sales['date']>=pd.to_datetime((easter(datetime.now().year, EASTER_ORTHODOX))-timedelta(days=6)))&
                            (sales['isdiscount']==0)].index
            sales.drop(sales_to_delete , inplace=True)

        if datetime.now().date().month == 9:
            icecream = set(sales[(sales['CLASS']==4774)&(sales['date']>=f'{datetime.now().year}-09-01')&(sales['PromoTypeCode']!=0)]['item_id'])
            sales = sales[~(sales['item_id'].isin(icecream)&
                          (sales['date']<f'{datetime.now().year}-09-01'))]

        #удалить новогодние продажи
        if (datetime.now() - pd.to_datetime(f'{datetime.now().year-1}-12-31')).days<=28:
            sales = sales[~((sales['date']<=f'{datetime.now().year}-01-02')&(sales['date']>=f'{datetime.now().year-1}-12-24'))]

        #посчитать средние продажи по дням недели для яиц
        categories = load_categories(store_id)
        demand_distribution = sales[sales['SUBCLASS']==6025]
        demand_distribution = demand_distribution[demand_distribution['date'].isin(sorted(list(set(demand_distribution['date'])))[-28:])][['date', 'item_id', 'qnty']]
        demand_distribution['weekday'] = demand_distribution['date'].dt.weekday
        demand_distribution = demand_distribution.groupby(['date', 'weekday'], as_index = False)['qnty'].sum()
        demand_distribution = demand_distribution.groupby(['weekday'], as_index = False)['qnty'].mean()

        #получть максимальные и медианные продажи
        sales = get_max_and_median_sales(sales)

        #получить медианные и средние продажи
        sales = get_rolling_median_and_mean_sales(sales)

        sales['trend'] = sales['no_disc_rolling_median'] - sales['no_disc_rolling_median14']

        #преобразовать типы данных
        sales = type_conversion(sales)

        #добавить праздники
        sales = sales.merge(holidays, how ='left')

        #медианные продажи по дням недели
        sales = get_weekday_median_features(sales)
        sales.fillna(0, inplace = True)

        weekday_features = get_weekday_features(sales)
        #зануление темпа редких продаж
        zero_temp_items = sales[sales['qnty'] > 0].groupby('item_id', as_index = False).agg({'qnty' : 'count'})
        zero_temp_items = set(zero_temp_items[zero_temp_items['qnty'] < 3]['item_id'])

        sales = sales.set_index('date')
        features = sales.sort_index().drop_duplicates('item_id', keep='last')[['item_id', 'DIVISION', 'GROUP_NO',
                            'max_sales_no_disc', 'max_sales_disc', 'disc_median', 'disc_max',
                            'no_disc_rolling_median', 'no_disc_rolling_median14', 
                            'disc_rolling_median', 'disc_rolling_median14', 
                            'disc_rolling_mean7', 'no_disc_rolling_mean7',  'no_disc_rolling_min7',
                            'trend', 'price_base', 'DEPT', 'CLASS', 'SUBCLASS']]
        features['key'] = 1

        #объединить фичи
        df = get_dataset(features, holidays, weekday_features, discounts)

        #зануление темпа редких продаж      
        df.loc[df['item_id'].isin(zero_temp_items), ['no_disc_rolling_median', 'no_disc_rolling_median14', 
                                                     'no_disc_rolling_mean7',  'no_disc_rolling_min7', 'weekday_rolling_median', 'trend']] = 0
        
        x = get_x(df.drop(['no_disc_rolling_min7'], axis = 1))
        del sales

        #загрузить модель
        model = load_model(store_id)

        #сделать прогноз
        prediction_df = get_predict(model, df, x)

        #ограничения прогноза
        categories = load_categories(store_id).drop('date', axis=1)
        prediction_df = limiting_nondiscont_prediction(prediction_df, df, categories)
        prediction_df = limiting_discont_prediction(prediction_df, df)
        prediction_df = limiting_lower_discont_prediction(prediction_df)
        prediction_df = the_lower_limit(prediction_df)

        ###### поправка на класс ######
        prediction_df = prediction_df.merge(categories)
        prediction_df['weekday'] = prediction_df['date'].dt.weekday
        prediction_class_total = prediction_df[prediction_df['SUBCLASS'] == 6025]
        prediction_class_total = prediction_class_total.groupby(
            ['date', 'weekday', 'SUBCLASS'], as_index = False)['prediction'].sum().rename(columns = {'prediction':'qnty_pred'})
        prediction_class_total = prediction_class_total.merge(demand_distribution)
        prediction_class_total['difference'] = prediction_class_total['qnty'] - prediction_class_total['qnty_pred']
        prediction_class_total = prediction_class_total[['date', 'SUBCLASS', 'difference', 'qnty', 'qnty_pred']]
        prediction_class_total = prediction_class_total[prediction_class_total['difference']>0]
        prediction_df = prediction_df.merge(prediction_class_total, on = ['date', 'SUBCLASS'], how = 'left')

        pred_mask = prediction_df['qnty'].notnull()
        prediction_df.loc[pred_mask, 'prediction'] = (
            prediction_df.loc[pred_mask, 'prediction']  + ( (prediction_df.loc[pred_mask,'prediction'] *
                                                             prediction_df.loc[prediction_df['qnty'].notnull(),'difference']) /
                                                           prediction_df.loc[prediction_df['qnty'].notnull(),'qnty_pred']
                                                          )
        )

        zero_forecast = reader_csv('spardata', r'zero_forecast.csv')
        zero_forecast['zero_forecast'] = 0
        zero_forecast['store_id'] = zero_forecast['store_id'].astype(str)

        if len(zero_forecast[zero_forecast['store_id'] == store_id]) > 0:
            zero_forecast = zero_forecast[zero_forecast['store_id'] == '1347'][['SUBCLASS', 'zero_forecast']]
            prediction_df = prediction_df.merge(zero_forecast, how = 'left')
            pred_mask1 = prediction_df['zero_forecast'].notnull()
            prediction_df.loc[pred_mask1, 'prediction'] = prediction_df.loc[pred_mask1, 'zero_forecast']


        prediction_df = prediction_df[['date', 'item_id', 'prediction', 'isdiscount']]
        prediction_df = prediction_df.merge(categories[['item_id', 'CLASS']])

        #сезонные коэффициенты
        prediction_df = season_coefficient(prediction_df)

        prediction_df.loc[prediction_df['prediction'] < 0, 'prediction'] = 0
        prediction_df = prediction_df[['date', 'item_id', 'prediction', 'isdiscount']]
        prediction_df = prediction_df.drop_duplicates(subset = ['date', 'item_id'])

        #поправки на прогноз погоды
        prediction_df = prediction_df.merge(categories[['item_id', 'CLASS']], how = 'left')
        prediction_df = prediction_df.merge(coef, on = ['date', 'item_id'], how = 'left')
        prediction_df.loc[~prediction_df['coef'].isna(), 'prediction'] = prediction_df.loc[~prediction_df['coef'].isna(), 'prediction']* \
                                                                        prediction_df.loc[~prediction_df['coef'].isna(), 'coef']
        prediction_df = prediction_df.drop(['CLASS', 'coef'], axis=1)

        #поправки на праздники
        try:
            holiday_params = reader_csv('holidays', r'{}/coef.csv'.format(store_id))

            production_calendar = reader_csv('holidays', r'production_calendar/{}.csv'.format(datetime.now().year))
            production_calendar['date'] = pd.to_datetime(production_calendar['date'])

            prediction_df = prediction_df.merge(production_calendar, how='left')

            prediction_df = prior_year_sales_amendens(prediction_df, holiday_params)
            print('ok')
        except Exception as e:
            print(e)

        prediction_df['item_id'] = prediction_df['item_id'].astype(int)
        prediction_df = prediction_df[['date', 'item_id', 'prediction']]
        prediction_df = get_onpromo_prediction(store_id, prediction_df)
        prediction_df = zero_forecasting_subclass(prediction_df, store_id)

        month_plan = reader_csv('spardata', 'month_plan.csv')
        if int(store_id) in set(month_plan['ObjCode']):
            result = get_prediction_from_month_plan(int(store_id), month_plan)
            prediction_df = prediction_df[~prediction_df['item_id'].isin(result['item_id'])]
            prediction_df = pd.concat([prediction_df, result])

#         # поправка на недопрогноз из-за анлимально больших продаж
#         prediction_df = get_correct_prediction(pred_sales=prediction_df, store_id=int(store_id))

        prediction_df = prediction_df[~prediction_df['item_id'].isin(remove_items_from_forecast)]
        # сдвиг дат прогнозов из-за вечерних поставок в магазин
        shift_stores = [862, 859, 860, 848, 846, 805, 764]
        shift_stores1 = [782, 1371]
        if int(store_id) in shift_stores:
            ContractCode = [70018347,70011642,70011513,151579,70022684,70008640,324062,70006516,70022390,70026675,70024124,70011310,
                                    70025631,70013565,315938,70025939,70026890,70026036,70022381,306947,70011884,70018346,70013733]
            date_shift_items = parse_provider(store_id)
            date_shift_items = date_shift_items[date_shift_items['ContractCode'].isin(ContractCode)&(date_shift_items['ContragentCode'] == 200)]['Item'].tolist()

            prediction_df1 = prediction_df.copy()
            prediction_df1['date'] = pd.to_datetime(prediction_df1['date'])
            prediction_df1.loc[prediction_df1['item_id'].isin(date_shift_items), 'date'] = prediction_df1.loc[prediction_df1['item_id'].isin(date_shift_items), 'date'] - timedelta(1)
            # приведение даты к нормальному формату
            prediction_df1['date'] = prediction_df1['date'].astype(str)
            prediction_df1['date'] = prediction_df1['date'].map(lambda x: x[:10])

            # перенос 8 марта на 7 марта
            prediction_df_07_03 = prediction_df1[prediction_df1['date'] == '2023-03-07']
            prediction_df_07_03.loc[prediction_df_07_03['date'] == '2023-03-07', 'date'] = '2023-03-08'
            prediction_df1 = prediction_df1[prediction_df1['date'] != '2023-03-07']
            prediction_df1.loc[prediction_df1['date'] == '2023-03-08', 'date'] = '2023-03-07'
            prediction_df1 = pd.concat([prediction_df1, prediction_df_07_03])
            prediction_df1 = prediction_df1.sort_values('date')
            put_dfto_cloud(prediction_df1, container_name, r'predictions/{}/prediction.csv'.format(store_id))

            # приведение даты к нормальному формату
            prediction_df['date'] = prediction_df['date'].astype(str)
            prediction_df['date'] = prediction_df['date'].map(lambda x: x[:10])

        elif store_id in shift_stores1:
            prediction_df1 = prediction_df.copy()
            date_shift_items = parse_provider(store_id)
            date_shift_items = date_shift_items[(date_shift_items['ContractCode'] == 151579)]['Item'].tolist()
            prediction_df1['date'] = prediction_df1['date'].astype(str)
            prediction_df1['date'] = prediction_df1['date'].map(lambda x: x[:10])

            # перенос 8 марта на 7 марта
            prediction_df_07_03 = prediction_df1[(prediction_df1['date'] == '2023-03-07')&(prediction_df1['item_id'].isin(date_shift_items))]
            prediction_df_07_03.loc[prediction_df_07_03['date'] == '2023-03-07', 'date'] = '2023-03-08'
            prediction_df1 = prediction_df1[~((prediction_df1['date'] == '2023-03-07')&prediction_df1['item_id'].isin(date_shift_items))]
            prediction_df1.loc[(prediction_df1['date'] == '2023-03-08')& prediction_df1['item_id'].isin(date_shift_items), 'date'] = '2023-03-07'
            prediction_df1 = pd.concat([prediction_df1, prediction_df_07_03])

            prediction_df1 = prediction_df1.sort_values(['item_id', 'date'])
            put_dfto_cloud(prediction_df1, container_name, r'predictions/{}/prediction.csv'.format(store_id))
            

        else:
            # приведение даты к нормальному формату
            prediction_df['date'] = prediction_df['date'].astype(str)
            prediction_df['date'] = prediction_df['date'].map(lambda x: x[:10])
            prediction_df = prediction_df.sort_values('date')
            put_dfto_cloud(prediction_df, container_name, r'predictions/{}/prediction.csv'.format(store_id))

      

        last_date = (datetime.now() + timedelta(5)).strftime('%Y-%m-%d')
        output1 = prediction_df[prediction_df['date'] <= last_date]
        put_dfto_cloud(output1, 'predhist', r'{}/{}.csv'.format(store_id, datetime.now().date()))

        del discounts
        
    except Exception as err:
        traceback.print_exception(*sys.exc_info())
        message = (
            f"""
            Error in prediction store_id: {store_id}
            Scheduled: {datetime.now()}
            """ + 
            traceback.format_exc()
            )
        telegram_bot_sendtext(message)
