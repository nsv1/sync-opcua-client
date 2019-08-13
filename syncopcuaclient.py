# -*- coding: utf-8 -*-
# import sys
from influxdb import InfluxDBClient
from datetime import datetime
from opcua import Client
from settings import readsettings, encrypt
import concurrent.futures


class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """

    def datachange_notification(self, node, val, data):
#        print(
#            "Python: New data change event", node, val,
#            node.nodeid.to_string(),
#            data.monitored_item.Value.SourceTimestamp,
#            data.monitored_item.Value.StatusCode.name
#              )

        block, measurement = nodes[node.nodeid.to_string()]
#        print(block, measurement)
        status = data.monitored_item.Value.StatusCode.name
        timestamp = data.monitored_item.Value.SourceTimestamp.strftime(
                "%Y-%m-%dT%H:%M:%S.%f")
        json_body = [{
            "measurement": measurement,
            "tags": {"block": block},
            "time": timestamp,
            "fields": {"value": val,
                       "quality": status}
            }]
        client_db.write_points(json_body)
#        print(block, measurement, val, status)
#
#    def event_notification(self, event):
#        print("Python: New event", event)


if __name__ == "__main__":

    settings, nodes = readsettings('settings.yaml')
    # Set OPC UA server param
    url = settings['opcua_server']['url']
    ua_username = settings['opcua_server']['username']
    ua_password = encrypt(settings['opcua_server']['password'])
    ua_period = settings['opcua_server']['period_subsription']
    # Set inflaxdb param
    host = settings['influxdb']['host']
    port = settings['influxdb']['port']
    username = settings['influxdb']['username']
    password = encrypt(settings['influxdb']['password'])
    mydb = settings['influxdb']['mydb']
#    username = settings['server_opcua']['username']
#    psw = settings['server_opcua']['password'].encode()
#    password = base64.b64decode(psw).decode()

    myvar = []

    client_db = InfluxDBClient(host=host, port=port,
                               username=username, password=password)

    listdb = client_db.get_list_database()
    client_db.create_database(mydb)
    client_db.switch_database(mydb)

    client_ua = Client(url)
    try:
        client_ua.set_user(ua_username)
        client_ua.set_password(ua_password)
        client_ua.connect()
        print('Client Connected')
        for i in nodes:
            myvar.append(client_ua.get_node(i))
        handler = SubHandler()
        sub = client_ua.create_subscription(ua_period, handler)
        handle = sub.subscribe_data_change(myvar)
        print('Subscribe data_change with period {} ms'.format(ua_period))
#        time.sleep(5)
        input("Press Enter to continue...")
        for i in handle:
            sub.unsubscribe(i)
            print('Unsubscribe handle: ', i)
        print('Start deleting Sub')
        sub.delete()

    finally:
        try:
            client_db.close()
            client_ua.disconnect()
        except concurrent.futures._base.TimeoutError:
            print('TimeoutError')
        print('Client Disconnected')
