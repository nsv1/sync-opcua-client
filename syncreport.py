# -*- coding: utf-8 -*-
from influxdb import DataFrameClient #, InfluxDBClient
#from datetime import datetime, timedelta, timezone
from settings import Set
import numpy as np
import pandas as pd
import os


def check_file(path, block, date_hour):
    path += '/{}{}'.format(block, date_hour.strftime('/%Y/%m/%d'))
    file = '{}{}.txt.zip'.format(block, date_hour.strftime('%Y%m%d%H'))
    return True if file in os.listdir(path) else False


def make_df(block, date_hour):
    block = "01"
    dt = date_hour.strftime("%Y-%m-%dT%H:%M:%SZ")
    query_where = "select value, quality \
                    from frequency, power, target_power, quality \
                    where block='{0}';".format(block, dt)
#    query_where = "select value, quality from power, frequency \
#                    where block='{0}' and time>'{1}'-0.5s and time<'{1}'+1h+0.5s;".format(block, dt)

    dict_df = client.query(query_where)
    result = pd.DataFrame(index=range(3600))
    for i in dict_df:
        df = dict_df[i]
        # округляем до секунд
        df.index = df.index.round('S')
        # приводим к сеундам
        df.index -= date_hour
        df.index = df.index.seconds
        # группируем и находим среднее значение в группе
        dfi = df.pivot_table(values='value', index=df.index, 
                       aggfunc=np.mean, fill_value=0)
        # присоединяем dfi к результату по индексам
        result = result.join(dfi)
        result = result.rename(columns={"value": i})
    print(result)
    return result

def make_report():
#    now = datetime.utcnow()
    now = pd.Timestamp.now('UTC')
    # Определение текущей даты и часа со смещением часа (shift+1)
    date_hour = now.replace(minute=0, second=0, microsecond=0, nanosecond=0)
#    date_time = now - timedelta(
#            hours=1,
#            minutes=now.minute,
#            seconds=now.second,
#            microseconds=now.microsecond)
    for i in blocks:
        make_df(i, date_hour)
#        if not check_file(path, i, date_hour):
#            make_file(i, date_hour)
    

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

