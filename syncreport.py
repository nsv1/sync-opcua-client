# -*- coding: utf-8 -*-
from influxdb import InfluxDBClient, DataFrameClient
from datetime import datetime, timedelta
from settings import readsettings, encrypt
import numpy as np
import pandas as pd
import os


def check_file(path, block, date_time):
    path += '/{}{}'.format(block, date_time.strftime('/%Y/%m/%d'))
    file = '{}{}.txt.zip'.format(block, date_time.strftime('%Y%m%d%H'))
    return True if file in os.listdir(path) else False


def make_file(block, date_time):
    block = '01'
    dt = date_time.strftime("%Y-%m-%dT%H:00:00Z")
    query_where = "select value, quality from power, quality \
                    where block='{0}';".format(block, dt)
#    query_where = "select value, quality from power, frequency \
#                    where block='{0}' and time>'{1}'-0.5s and time<'{1}'+1h+0.5s;".format(block, dt)

    result = client.query(query_where)
#    result.raw
#    y = result.items()
#    y = [i for i in y[0][1]]
#    print(y)
    res = pd.DataFrame(index=[str(i) for i in range(3600)])
    for i in result:
        df = result[i]
        # округляем до секунд
        df.index = df.index.round('S')
        # приводим к сеундам
        df.index = df.index - date_time.timestamp()
        # группируем и находим среднее значение в группе
        df.pivot_table(values='value', index=df.index, 
                       aggfunc=np.mean, fill_value=0)
        
        
        grouped = df.groupby(df.index)
        res = grouped['value'].agg(np.mean) # Результат - Series
        res.index = res.index.strftime('%S')
        res.to_frame() # Результат - DataFrame
        

def make_report():
    now = datetime.now()
    # Определение текущей даты и часа со смещением часа (shift+1)
    date_time = now - timedelta(
            hours=timeshift+1,
            minutes=now.minute,
            seconds=now.second,
            microseconds=now.microsecond)
    for i in blocks:
        if not check_file(path, i, date_time):
            make_file(i, date_time)


if __name__ == "__main__":
    
    settings, node_id_lst = readsettings('settings.yaml')
    host = settings['influxdb']['host']
    port = settings['influxdb']['port']
    username = settings['influxdb']['username']
    password = encrypt(settings['influxdb']['password'])
    mydb = settings['influxdb']['mydb']
    path = settings['syncreport']['filestorige']['path']
    timeshift = settings['syncreport']['timeshift']
    blocks = settings['opcua_server']['block']

#    client = InfluxDBClient(host=host, port=port,
#                            username=username, password=password)
    client = DataFrameClient(host=host, port=port,
                            username=username, password=password)

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

