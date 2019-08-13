# -*- coding: utf-8 -*-
import base64
import yaml


def readsettings(file):
    nodeids = {}
    doc = open_file(file)
    settings = yaml.load(doc, Loader=yaml.FullLoader)
    for i in settings['opcua_server']['block']:
        for j in settings['opcua_server']['block'][i]:
            for k in settings['opcua_server']['block'][i][j]['node_id']:
                if k is not None:
                    nodeids[k] = (i, j)
    return settings, nodeids


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

#s, n = readsettings('settings.yaml')
#print(s)
#print(n)
