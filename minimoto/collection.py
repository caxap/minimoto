#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

import inspect
import UserDict
from models.field import Field, ListField
from models.manager import MotorManager


isfield = lambda x: isinstance(x, Field)
_is_collections_field = lambda x: isinstance(x, ListField) and \
    x.item_type and issubclass(x.item_type, Collection)


def inspect_fields(collection_class):
    if not hasattr(collection_class, '__fields__'):
        collection_class.__fields__ = fields = {}
        for name in dir(collection_class):  # with inherited props
            if name.startswith('__'):
                continue
            attr = getattr(collection_class, name)
            if isfield(attr):
                fields[name] = attr
    return collection_class.__fields__


def reset_lazy_classes():
    global __lazy_classes__
    __lazy_classes__ = {}


__lazy_classes__ = {}


class CollectionMetaClass(type):

    def __new__(cls, name, bases, attrs):
        global __lazy_classes__
        super_new = super(CollectionMetaClass, cls).__new__
        parents = [b for b in bases if hasattr(b, '__mro__')]
        field_names = []

        # update field name only if it's not given
        for attr_name, attr in attrs.items():
            if isfield(attr):
                if not attr.name:
                    attr.name = attr_name
                field_names.append(attr.name)

        # process inherited fields
        for base in parents:
            for attr_name, attr in inspect.getmembers(base, isfield):
                if not attr.name:
                    attr.name = attr_name
                if attr.name in field_names:
                    raise TypeError(
                        'Field "%r" in %r class conflicts with field with '
                        'same name from base %r class.' %
                        (attr.name, name, base.__name__))
                field_names.append(attr.name)

        new_class = super_new(cls, name, bases, attrs)

        # use default manager if other is not given
        objects = attrs.get('objects')
        if objects is None:
            if not new_class.__manager__:
                raise TypeError('Manager not found for "%s" class' %
                                (new_class.__name__,))
            new_class.objects = new_class.__manager__(collection=new_class)
        elif objects.collection is None:
            objects.collection = new_class

        __lazy_classes__[name] = new_class
        return new_class

    @property
    def m(cls):
        '''
        Shortcut to access collection manager.
        '''
        return cls.objects


#
# Actually collection.MutableMapping should be used here instead of old
# DictMixin. But it cannot due to collision with our metaclass definition.
#
class DocumentMixin(UserDict.DictMixin):

    def __getitem__(self, name):
        if name in inspect_fields(self.__class__):
            return getattr(self, name)
        raise KeyError('Collection "%s" has no field "%s".' %
                       (self.collection_name(), name))

    def __setitem__(self, name, value):
        if name in inspect_fields(self.__class__):
            return setattr(self, name, value)
        raise KeyError('Collection "%s" has no field "%s".' %
                       (self.collection_name(), name))

    def __delitem__(self, name):
        raise NotImplementedError(
            'Operation is not supported for collection type.')

    def __contains__(self, name):
        return name in inspect_fields(self.__class__)

    def keys(self):
        return inspect_fields(self.__class__).keys()

    def __hash__(self):
        if getattr(self, '_id', None):
            return hash(self._id)
        raise NotImplementedError(
            'Operation is not supported for collection w/o "_id" field.')

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        self_id = getattr(self, '_id')
        other_id = getattr(self, '_id')
        if self_id or other_id:
            return self_id == other_id
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class Collection(object, DocumentMixin):

    __metaclass__ = CollectionMetaClass
    __manager__ = MotorManager
    __collection__ = None

    def __new__(cls, class_name=None, *args, **kwargs):
        if class_name:
            global __lazy_classes__
            return __lazy_classes__.get(class_name)
        return super(Collection, cls).__new__(cls, *args, **kwargs)

    @classmethod
    def collection_name(cls):
        if cls.__collection__:
            return cls.__collection__
        return cls.__name__.lower()

    @classmethod
    def null(cls, **data):
        '''
        Will creatre null object that can be used as default value for
        embedded collections.
        '''
        return cls.create(data)

    def __init__(self, *args, **kwargs):
        super(Collection, self).__init__()
        self._data = {}
        self.update(*args, **kwargs)

    def validate(self, validate_embedded=False):
        #TODO: reduce boilerplate code to traverse document tree
        fields = inspect_fields(self.__class__)
        missing = []
        for name, field in fields.iteritems():
            value = getattr(self, name)
            if field.required and field.is_empty(value):
                missing.append(field.name)
            elif validate_embedded:
                if isinstance(value, Collection):
                    value.validate()
                elif value and isinstance(field, ListField) and \
                        issubclass(field.item_type, Collection):
                    [v.validate() for v in value]
        if missing:
            raise ValueError(
                'Required fields %s must have non-empty values.' % (missing,))

    def as_dict(self, exclude_unset=False):
        fields = inspect_fields(self.__class__)
        data = {}
        for name, field in fields.iteritems():
            value = getattr(self, name)
            if exclude_unset and not field.required and \
                    field.name not in self._data:
                continue
            if isinstance(value, Collection):
                value = value.as_dict(exclude_unset)
            elif value and _is_collections_field(field):
                value = [v.as_dict(exclude_unset) for v in value]
            data[name] = value
        # Because we don't want pass empty `_id` to client code
        if not data.get('_id'):
            data.pop('_id', None)
        return data

    @classmethod
    def create(cls, raw_data, strict=True):
        data = {}
        for name, value in raw_data.iteritems():
            try:
                data[str(name)] = value
            except:
                if strict:
                    raise
        return cls(**data)

    def __str__(self):
        return '<%s: %s>' % (self.collection_name(), self._data)
