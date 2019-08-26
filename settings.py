# -*- coding: utf-8 -*-
import base64
import yaml


def readsettings(file):
    doc = open_file(file)
    settings = yaml.load(doc, Loader=yaml.FullLoader)
    return settings


def nodes(settings):
    nodeids = {}
    for i in settings['opcua_server']['block']:
        for j in settings['opcua_server']['block'][i]:
            for k in settings['opcua_server']['block'][i][j]['node_id']:
                if k is not None:
                    nodeids[k] = (i, j)
    return nodeids


def set_ranges(settings):
    ranges = {}
    for i in settings['opcua_server']['block']:
        for j in settings['opcua_server']['block'][i]:
            for k in settings['opcua_server']['block'][i][j]['range']:
                ranges[(i, j, k)] = settings['opcua_server']['block'][i][j]['range'][k]
    return ranges


def write(file):
    pass


def filter_lst(lst):
    return list(filter(lambda x: x is not None, lst))


def open_file(file, param='r'):
    if file is None:
        print('No file selected')
    else:
        try:
            with open(file, param) as f:
                return f.read()
        except UnicodeDecodeError:
            print('File {} not coding in UTF-8'.format(file))


def encrypt(psw, param='decode'):
    '''
    For encode psw use any string
    '''
    if psw is not None:
        psw = psw.encode()
        try:
            if param == 'decode':
                psw = base64.b64decode(psw).decode()
            else:
                psw = base64.b64encode(psw).decode()
        except:
            psw = None
            print('Wrong password')
        return psw


class Set:
    '''
    Atribut class set
    '''
    settings = readsettings('./settings/settings.yaml')
    # OPC UA
    ua_url = settings['opcua_server']['url']
    ua_username = settings['opcua_server']['username']
    ua_password = encrypt(settings['opcua_server']['password'])
    ua_period = settings['opcua_server']['period_subsription']
    ua_timeshift = settings['opcua_server']['timeshift']
    ua_nodes = nodes(settings)
    # Range of nodes
    ranges = set_ranges(settings)
    # INFLUXDB
    infl_host = settings['influxdb']['host']
    infl_port = settings['influxdb']['port']
    infl_username = settings['influxdb']['username']
    infl_password = encrypt(settings['influxdb']['password'])
    infl_mydb = settings['influxdb']['mydb']
    # SyncReport
    sr_path = settings['syncreport']['filestorige']['path']
    sr_timeshift = settings['syncreport']['timeshift']
    sr_blocks = settings['opcua_server']['block']
    sr_sequence = settings['syncreport']['sequence']
    
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#def readsettings(file):
#    nodeids = {}
#    doc = open_file(file)
#    settings = yaml.load(doc, Loader=yaml.FullLoader)
#    for i in settings['opcua_server']['block']:
#        for j in settings['opcua_server']['block'][i]:
#            for k in settings['opcua_server']['block'][i][j]['node_id']:
#                if k is not None:
#                    nodeids[k] = (i, j)
#    return settings, nodeids
#
#
#def write(file):
#    pass
#
#
#def filter_lst(lst):
#    return list(filter(lambda x: x is not None, lst))
#
#
#def open_file(file, param='r'):
#    if file is None:
#        print('No file selected')
#    else:
#        try:
#            with open(file, param) as f:
#                return f.read()
#        except UnicodeDecodeError:
#            print('File {} not coding in UTF-8'.format(file))
#
#
#def encrypt(psw, param='decode'):
#    '''
#    For encode psw use any string
#    '''
#    if psw is not None:
#        psw = psw.encode()
#        try:
#            if param == 'decode':
#                psw = base64.b64decode(psw).decode()
#            else:
#                psw = base64.b64encode(psw).decode()
#        except:
#            psw = None
#            print('Wrong password')
#        return psw

#s, n = readsettings('settings.yaml')
#print(s)
#print(n)
