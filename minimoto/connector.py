#!/usr/bin/env python
# -*- coding: utf-8 -*-

import motor
import pymongo


_connection = None
_db = None


class ConnectionError(Exception):
    pass


def current_connection():
    if _connection is None:
        raise ConnectionError("Database connection isn't initialised")
    return _connection


def current_db():
    if _db is None:
        raise ConnectionError("Database isn't connected")


def get_connection(**kwargs):
    global _connection

    reconnect = kwargs.pop('reconnect', None)
    if reconnect:
        disconnect()
    elif _connection is not None:
        raise ConnectionError("Database connection has been already created")

    connection_class = kwargs.pop('connection_class', motor.MotorClient)
    if 'replicaSet' in kwargs:
        if 'hosts_or_uri' not in kwargs:
            kwargs['hosts_or_uri'] = kwargs.pop('host', None)
        kwargs.pop('port', None)
        if not isinstance(kwargs['replicaSet'], basestring):
            kwargs.pop('replicaSet', None)
        connection_class = kwargs.pop('replica_connection_class',
                                      motor.MotorReplicaSetClient)
    try:
        _connection = connection_class(**kwargs)
    except Exception, e:
        raise ConnectionError("Cannot connect to database: %s" % (e, ))
    return _connection


def disconnect():
    global _connection

    if _connection is not None:
        _connection.disconnect()
        _connection = None


def connect(db, **kwargs):
    global _db

    connection = get_connection(**kwargs)
    _db = connection[db]
    return _db


def connect_sync(db, **kwargs):
    kwargs['connection_class'] = pymongo.MongoClient
    kwargs['replica_connection_class'] = pymongo.MongoReplicaSetClient
    return connect(db, **kwargs)
