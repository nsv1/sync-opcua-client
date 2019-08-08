# -*- coding: utf-8 -*-
from influxdb import InfluxDBClient
from datetime import datetime
from settings import readsettings, encrypt


settings, node_id_lst = readsettings('settings.yaml')
host = settings['influxdb']['host']
port = settings['influxdb']['port']
username = settings['influxdb']['username']
password = encrypt(settings['influxdb']['password'])
mydb = settings['influxdb']['mydb']

client = InfluxDBClient(host=host, port=port,
                        username=username, password=password)

listdb = client.get_list_database()
print(listdb)
if mydb not in listdb:
    client.create_database(mydb)
client.switch_database(mydb)
now = datetime.now()
now = now.strftime("%Y-%m-%dT%H:%M:%S.%f")

json_body = [
    {
        "measurement": "power",
        "tags": {
            "station": "ST01",
            "blok": "01"
        },
        "time": now,
        "fields": {
            "value": 127.,
            "quality": "good"
        }
    },
    {
        "measurement": "frequncy",
        "tags": {
            "station": "ST01",
            "blok": "01"
        },
        "time": now,
        "fields": {
            "value": 3000.01,
            "quality": "good"
        }
    },
    {
        "measurement": "target_power",
        "tags": {
            "station": "ST01",
            "blok": "01"
        },
        "time": now,
        "fields": {
            "value": 130.,
            "quality": "good"
        }
    },
    {
        "measurement": "quality",
        "tags": {
            "station": "ST01",
            "blok": "01"
        },
        "time": now,
        "fields": {
            "value": 130.,
            "quality": "good"
        }
    }
]

client.write_points(json_body)
results = client.query('SELECT * FROM "power"')
points = results.get_points(tags={'station': 'ST01'})
results.raw
