# -*- coding: utf-8 -*-
from influxdb import InfluxDBClient
from datetime import datetime
from settings import readsettings, encrypt


def make_report(client):
    


if __name__ == "__main__":
    
    settings, node_id_lst = readsettings('settings.yaml')
    host = settings['influxdb']['host']
    port = settings['influxdb']['port']
    username = settings['influxdb']['username']
    password = encrypt(settings['influxdb']['password'])
    mydb = settings['influxdb']['mydb']

    client = InfluxDBClient(host=host, port=port,
                            username=username, password=password)

    listdb = client.get_list_database()
    listdb = [i['name'] for i in listdb]
    print(listdb)
    if mydb not in listdb:
        print('В influxdb нет БД {}'.format(mydb))
    else:
        client.switch_database(mydb)
        make_report(client)

