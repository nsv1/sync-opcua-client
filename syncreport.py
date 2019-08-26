# -*- coding: utf-8 -*-
from influxdb import DataFrameClient #, InfluxDBClient
#from datetime import datetime, timedelta, timezone
from settings import Set
import numpy as np
import pandas as pd
import os
from zipfile import ZipFile
#import sched
import time
import schedule


def get_time_with_delta(delta='0h', tz=None):
    # Определение текущей даты и часа со смещением часа на -delta
    now = pd.Timestamp.now(tz)
    date_hour = now.replace(minute=0, second=0, microsecond=0, nanosecond=0)
    date_hour += pd.Timedelta(delta)
    return date_hour


def make_path_file(path, block, date_hour):
    _path = '{}/{}{}'.format(path, block, date_hour.strftime('/%Y/%m/%d/'))
    zip_file = '{}{}.txt.zip'.format(
                        block, date_hour.strftime('%Y%m%d%H'))
    txt_file = '{}{}.txt'.format(
                        block, date_hour.strftime('%Y%m%d%H'))
    if not os.path.exists(_path):
        os.makedirs(_path)
    return (_path+zip_file, txt_file)


def check_quality(df, block, measurement):
    good = []
    for i in df.index:
        if (
            df.loc[i, 'quality'] != 'Good' or
            df.loc[i, 'value'] > Set.ranges[(block, measurement, 'max')] or
            df.loc[i, 'value'] < Set.ranges[(block, measurement, 'min')]):
            good.append(i)
    return good


def filling_quality(dfi, date_hour, block):
    pass


def filling_naan(df, date_hour, measurement, block):
    list_nan = []
    for i in df.index[::-1]:
        if np.isnan(df.loc[i, measurement]):
            list_nan.append(i)
            if i == 0:
                previos_dt = date_hour - pd.Timedelta(hours=1)
                name_files = make_path_file(path, block, previos_dt)
                try:
                    with ZipFile(name_files[0]) as myzip:
                        with myzip.open(name_files[1]) as myfile:
                            f = myfile.readlines()
                    # parsing last row
                    last_measur = f[-1].decode('utf8').split(':')[1].split(';')[0:4]
                    last_measur = dict(zip(Set.sr_sequence, map(float,last_measur)))
                    # set value
                    df.loc[list_nan, measurement] = last_measur[measurement]
                except FileNotFoundError:
                    pass
        elif len(list_nan):
            df.loc[list_nan, measurement] = df.loc[i, measurement]
            list_nan = []
    return df


def make_file(df, block, date_hour):

    name_files = make_path_file(path, block, date_hour)
    data = ''

    for index, row in df.iterrows():

#        import pdb
#        pdb.set_trace()
#
        data += '{}:'.format(index)
        for i in sequence:
            try:
                data += '{};'.format(row[i])
            except KeyError:
                data += ';'
        data += '\n'

    with open(name_files[1], 'w') as myfile:
        myfile.write(data)
    with ZipFile(name_files[0], 'w') as myzip:
        myzip.write(name_files[1])
    os.remove(name_files[1])
#    import pdb
#    pdb.set_trace()


def make_report():
    print('start ', pd.datetime.now())
    # Устанавливаем расчетное время
    date_hour = get_time_with_delta('{} hours'.format(timeshift), 'UTC')
    # Формируем dataframe для каждого блока
    for block in blocks:
        dt = date_hour.strftime("%Y-%m-%dT%H:%M:%SZ")
        query_where = "select value, quality \
                        from frequency, power, target_power, quality \
                        where block='{0}' and time>'{1}'-500ms \
                        and time<'{1}'+1h+500ms;".format(block, dt)
        dict_df = client.query(query_where)
        result = pd.DataFrame(index=range(3600))
        # Формируем dataframe для каждого параметра
        for i in dict_df:
            df = dict_df[i]
            # Ищем и Удаляем строки с плохим качеством и выходящие за диапазон min/max
            bad_quality = check_quality(df, block, i)
            df.drop(bad_quality)
            # округляем до секунд и приводим index к сеундам
            df.index = df.index.round('S')
            df.index -= date_hour
            df.index = df.index.seconds
            # решаем пробдему наличия кривых секунд после .seconds
#            list_index = df.index.to_list()
#            new_index = [(x if x < 3600 else x - 86400) for x in list_index]
#            df['new_id'] = new_index
#            df = df.set_index('new_id')
            # группируем и находим среднее значение в группе
            dfi = df.pivot_table(values='value', index=df.index,
                                 aggfunc=np.mean, fill_value=0)
            # присоединяем dfi к result по индексам
#            if i == 'quality':
#                dfi = filling_quality(dfi, date_hour, block)
            result = result.join(dfi)
            result = result.rename(columns={"value": i})
            # Заполняем пропуски
            filling_naan(result, date_hour, i, block)
        print(result)
        make_file(result, block, date_hour)


if __name__ == "__main__":

    mydb = Set.infl_mydb
    path = Set.sr_path
    timeshift = Set.sr_timeshift
    blocks = Set.sr_blocks
    sequence = Set.sr_sequence
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
    make_report()
    # Запускаем пеиодическое выполнление функции
#    schedule.every().minute.at(":17").do(make_report)
#
#    while True:
#        schedule.run_pending()
#        time.sleep(1)

#    import pdb
#    pdb.set_trace()

