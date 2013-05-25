#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
import contextlib
import functools
import motor

from tornado import gen
from tornado import stack_context

from connector import current_db


__all__ = ['safe_motor', 'BaseManager', 'MotorManager', 'MotorOp', ]


def safe_motor(async_func):
    '''
    The decorator intended to reduce boilerplate try/except code to handle
    exceptions for motor.Op yield point.
    '''
    @functools.wraps(async_func)
    def wrapper(*args, **kwargs):
        callback = kwargs.get('callback')

        @contextlib.contextmanager
        def catch_error():
            try:
                yield
            except Exception, e:
                logging.error('Exception in asynchronous operation', exc_info=True)
                callback(None, e)

        with stack_context.StackContext(catch_error) as deactivate:
            async_func(*args, **kwargs)
            deactivate()

    return wrapper


class MotorOp(object):

    def __init__(self, action, qualifier=None, modifier=None, as_model=False, hard=False):
        self.action = action
        self.qualifier = qualifier
        self.modifier = modifier
        self.as_model = as_model
        self.hard = hard

    @gen.engine
    def execute(self, manager, *args, **kwargs):
        assert 'callback' in kwargs
        callback = kwargs.pop('callback')
        modifier = kwargs.pop('modifier', self.modifier)
        as_model = kwargs.pop('as_model', self.as_model)
        hard = kwargs.pop('hard', self.hard)

        try:
            db = kwargs.pop('db', None)
            if db is None:
                db = current_db()
            dbc = db[manager.collection_name]

            # We assumed if `qualifier` is not given, that action doesn't
            # return cursor and should be executed asynchronously
            if not self.qualifier:
                result = yield motor.Op(
                    getattr(dbc, self.action), *args, **kwargs)

            else:
                cursor = getattr(dbc, self.action)(*args, **kwargs)
                # motor cursor works synchronously, so `modifier` can
                # preprocess it (e.g skip, sort or group).
                if cursor and modifier:
                    modified_cursor = modifier(cursor)
                    if modified_cursor:
                        cursor = modified_cursor
                result = yield motor.Op(getattr(cursor, self.qualifier))

            # Unfortunately we cannot use `as class` option for auto conversion
            # to model class, because mongo uses same class for top-level and
            # inner documents.
            if as_model:
                if isinstance(result, (list, tuple, set)):
                    result = manager.create(result)
                else:
                    result = manager.create_one(result, hard=hard)

            callback(result, None)
        except Exception, e:
            callback(None, e)

    @classmethod
    def bind(cls, *args, **kwargs):
        operation = cls(*args, **kwargs)

        @gen.engine
        def execute(manager, db, *argz, **kwargz):
            assert 'callback' in kwargz, '`callback` is required'

            callback = kwargz.pop('callback')
            kwargz['callback'] = stack_context.wrap(callback)
            operation.execute(manager, db, *argz, **kwargz)
        return execute

bind_op = MotorOp.bind


class BaseManager(object):

    def __init__(self, collection=None):
        self.collection = collection

    @property
    def collection_name(self):
        return self.collection.collection_name()

    def create_one(self, data, hard=False):
        if data:
            return self.collection.create(data)
        elif hard:
            return self.collection.create({})

    def create(self, data):
        return [self.collection.create(x) for x in data] if data else []

    def create_dicts(self, data, exclude_unset=False):
        return self.as_dicts(self.create(data), exclude_unset=exclude_unset)

    def as_dicts(self, data, exclude_unset=False):
        return [x.as_dict(exclude_unset=exclude_unset) for x in data]


class MotorManager(BaseManager):

    insert        = bind_op('insert')
    save          = bind_op('save')
    update        = bind_op('update')
    remove        = bind_op('remove')
    find          = bind_op('find', 'to_list', as_model=True)
    find_one      = bind_op('find_one', as_model=True)
    count         = bind_op('find', 'count')
    group         = bind_op('group')
    create_index  = bind_op('create_index')
    ensure_index  = bind_op('ensure_index')
    aggregate     = bind_op('aggregate')
    find_and_modify = bind_op('find_and_modify')

    @gen.engine
    def all(self, *args, **kwargs):
        self.find(*args, **kwargs)

    @gen.engine
    def one(self, *args, **kwargs):
        callback = kwargs.pop('callback')
        result = yield motor.Op(self.find, *args, **kwargs)
        if result and len(result) > 1:
            raise ValueError("Multiple results found.")
        result = result[0] if result else None
        callback(result, None)
