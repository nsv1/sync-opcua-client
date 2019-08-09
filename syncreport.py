# -*- coding: utf-8 -*-
from influxdb import InfluxDBClient
from datetime import datetime, timedelta
from settings import readsettings, encrypt
import os


def check_file(path, block, date_time):
    path += '/{}{}'.format(block, date_time.strftime('/%Y/%m/%d'))
    file = '{}{}.txt.zip'.format(block, date_time.strftime('%Y%m%d%H'))
    return True if file in os.listdir(path) else False


def make_file(block, date_time):
    query_where = 'select value, quality \
                    from power, target_power, frequency \
                    where block=$block;'
    bind_params = {'block': block}
    result = client.query(query_where, bind_params=bind_params)


def make_report():
    date_time = datetime.now() - timedelta(seconds=3600)
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
    path = settings['filestorige']['path']
    blocks = settings['opcua_server']['block']

    client = InfluxDBClient(host=host, port=port,
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

