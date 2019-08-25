# -*- coding: utf-8 -*-
# import sys
from influxdb import InfluxDBClient
from datetime import timedelta
from opcua import Client
from settings import Set
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

        block, measurement = Set.ua_nodes[node.nodeid.to_string()]
#        print(block, measurement)
        status = data.monitored_item.Value.StatusCode.name
        timestamp = data.monitored_item.Value.SourceTimestamp
        timestamp -= timedelta(hours=Set.ua_timeshift)
        timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")
#        print(timestamp)
        json_body = [{
            "measurement": measurement,
            "tags": {"block": block},
            "time": timestamp,
            "fields": {"value": val,
                       "quality": status}
            }]
        client_db.write_points(json_body)
#        print(timestamp)
#        print(block, measurement, val, status)

    def event_notification(self, event):
        print("Python: New event", event)


if __name__ == "__main__":

    myvar = []
    client_db = InfluxDBClient(
            host=Set.infl_host,
            port=Set.infl_port,
            username=Set.infl_username,
            password=Set.infl_password)

    listdb = client_db.get_list_database()
    client_db.create_database(Set.infl_mydb)
    client_db.switch_database(Set.infl_mydb)

    client_ua = Client(Set.ua_url)
    try:
        client_ua.set_user(Set.ua_username)
        client_ua.set_password(Set.ua_password)
        client_ua.connect()
        print('Client Connected')
        for i in Set.ua_nodes:
            myvar.append(client_ua.get_node(i))
        handler = SubHandler()
        sub = client_ua.create_subscription(Set.ua_period, handler)
        handle = sub.subscribe_data_change(myvar)
        print('Subscribe data_change with period {} ms'.format(Set.ua_period))
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
