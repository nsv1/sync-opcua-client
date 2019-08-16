# -*- coding: utf-8 -*-
from influxdb import DataFrameClient #, InfluxDBClient
#from datetime import datetime, timedelta, timezone
from settings import Set
import numpy as np
import pandas as pd
import os


def make_path(path, block, date_hour):
    block = "01"
    path += '/{}{}'.format(block, date_hour.strftime('/%Y/%m/%d'))
    os.makedirs(path)
    name_file = '{}{}.txt.zip'.format(block, date_hour.strftime('%Y%m%d%H'))
    return True if file in os.listdir(path) else False


def check_quality(df, block, measurement):
    q = []
    for i in df.index:
        if (
            df.loc[i, 'quality'] != 'Good' or
            df.loc[i, 'value'] > Set.ranges[(block, measurement, 'max')] or
            df.loc[i, 'value'] < Set.ranges[(block, measurement, 'min')]
            ):
            q.append(i)
    return q


def get_last_value(measurement):
#            FIX IT    
    pass


def filling_naan(df, measurement):
    list_nan = []
    for i in df.index[::-1]:
        if np.isnan(df.loc[i, measurement]):          
            list_nan.append(i)
            last_value = get_last_value(measurement)
            if i==0 and last_value:
                for j in list_nan:
                    df.loc[j, measurement] = last_value
        elif len(list_nan):
            for j in list_nan:
                df.loc[j, measurement] = df.loc[i, measurement]
            list_nan = []


def make_df(block, date_hour):
#    block = "01"
    dt = date_hour.strftime("%Y-%m-%dT%H:%M:%SZ")
    query_where = "select value, quality \
                    from frequency, power, target_power, quality \
                    where block='{0}' and time>'{1}'-500ms \
                    and time<'{1}'+1h+500ms;".format(block, dt)
    dict_df = client.query(query_where)
    result = pd.DataFrame(index=range(3600))
    for i in dict_df:
#        i=['frequency', 'power', 'target_power', 'quality']
        df = dict_df[i]
        # Ищем и Удаляем строки с плохим качеством и выходящие за диапазон min/max
        bad_quality = check_quality(df, block, i)
        df.drop(bad_quality)
        # округляем до секунд и приводим index к сеундам
        df.index = df.index.round('S')
        df.index -= date_hour
        df.index = df.index.seconds
        # группируем и находим среднее значение в группе
        dfi = df.pivot_table(values='value', index=df.index, 
                       aggfunc=np.mean, fill_value=0)
        # присоединяем dfi к result по индексам
        result = result.join(dfi)
        result = result.rename(columns={"value": i})
        # Заполняем пропуски
        filling_naan(result, i)
    print(result)
    return result

def make_report():
#    now = datetime.utcnow()
    now = pd.Timestamp.now('UTC')
    # Определение текущей даты и часа со смещением часа (shift+1)
    date_hour = now.replace(minute=0, second=0, microsecond=0, nanosecond=0)
    date_hour -= pd.Timedelta(hours=1)# сметить hours=1
    # Формируем df для каждого блока
    for i in blocks:
        make_df(i, date_hour)
#       make_path(path, i, date_hour):
#       make_file(i, date_hour)
    

if __name__ == "__main__":
    
    mydb = Set.infl_mydb
    path = Set.sr_path
    timeshift = Set.sr_timeshift
    blocks = Set.sr_blocks

#    client = InfluxDBClient(host=host, port=port,
#                            username=username, password=password)
    client = DataFrameClient(
            host=Set.infl_host,
            port=Set.infl_port,
            username=Set.infl_username,
            password=Set.infl_password)
    
    listdb = client.get_list_database()
    listdb = [i['name'] for i in listdb]
#    print(listdb)
    if mydb not in listdb:
        print('В influxdb нет БД {}'.format(mydb))
    else:
        client.switch_database(mydb)

# FixIt
# нужно запустить пеиодическое расписание

        make_report()

