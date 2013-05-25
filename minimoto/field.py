#-*- coding: utf-8 -*-

from dateutil import tz
from datetime import datetime
from bson import ObjectId


def show_help(class_or_obj):
    # to avoid cyclic imports
    from collection import inspect_fields
    fields = inspect_fields(class_or_obj)
    for field in fields.values():
        print str(field)


def _ensure_type(item, item_type, type_cast=None):
    if item_type in (int, long):
        item_type = (int, long)
    if item_type and isinstance(item, item_type):
        return item
    if type_cast is None:
        if item_type == (int, long):
            type_cast = int
        else:
            from collection import Collection
            if issubclass(item_type, Collection):
                type_cast = item_type.create
            else:
                type_cast = item_type
    return type_cast(item)


def _ensure_parent(item, parent, _meta_field='__parent__'):
    from collection import Collection
    if isinstance(item, Collection) and isinstance(parent, Collection) and \
            not getattr(item, _meta_field, None):
        setattr(item, _meta_field, parent)
    return item


class _Default(object):
    __slots__ = ()
    __nonzero__ = lambda self: False
    __bool__ = __nonzero__


_DEFAULT = _Default()

# XXX: Not exactly immutable, but non-container types
# TODO: Add datetime ?
_IMMUTABLE_TYPES = (int, float, long, complex, bool, tuple, str, unicode)


class Field(object):

    field_type = None

    def __init__(self, default=_DEFAULT, name=None, field_type=None,
                 validators=None, required=False, choices=None, doc=None):
        self.name = name
        self.default = default
        self.field_type = field_type or self.field_type
        if validators and not isinstance(validators, (list, tuple)):
            validators = [validators, ]
        self.validators = validators
        self.required = required
        self.choices = choices
        self.doc = doc

    def is_empty(self, value):
        return value is None

    def validate(self, value, obj=None):
        if value is not None and self.field_type is not None and \
                not isinstance(value, self.field_type):
            try:
                value = _ensure_type(value, self.field_type)
            except TypeError:
                raise ValueError('Field "%s" must be %s, not %s.' %
                                 (self.name, self.field_type, type(value)))

        if not self.is_empty(value):
            if self.choices and value not in self.choices:
                raise ValueError('Field "%s" is "%r"; must be one of %r.' %
                                 (self.name, value, self.choices))
        elif self.required:
            raise ValueError('Field "%s" is required, but got %s.' %
                             (self.name, value))

        if self.validators is not None:
            [v(value) for v in self.validators]

        return value

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        value = obj._data.get(self.name, _DEFAULT)
        if value is _DEFAULT:
            if self.default is _DEFAULT:
                return None
            value = self.default() if callable(self.default) else self.default
            if not isinstance(value, _IMMUTABLE_TYPES):
                self.__set__(obj, value)
        return value

    def __set__(self, obj, value):
        obj._data[self.name] = self.validate(value, obj)

    def __str__(self):
        type_name = getattr(self.field_type, '__name__', self.field_type)
        meta = [
            ('name', self.name),
            ('type', type_name),
        ]
        if self.default is not None:
            meta.append(('default', self.default))
        if self.required:
            meta.append(('required', self.required))
        if self.choices:
            meta.append(('choices', self.choices))
        text = ', '.join(['%s: %s' % x for x in meta])
        text = '<%s [%s]>' % (self.__class__.__name__, text)
        if self.doc:
            text = '%s - %s' % (text, self.doc)
        return text


class StringField(Field):

    field_type = unicode

    def __init__(self, regex=None, min_length=None, max_length=None, *args, **kwargs):
        self.regex = regex
        self.max_length, self.min_length = max_length, min_length
        self.encoding = kwargs.pop('encoding', 'utf-8')
        super(StringField, self).__init__(*args, **kwargs)

    def is_empty(self, value):
        return value is None

    def validate(self, value, obj=None):
        # Actually most of python objects can be converted to string,
        # so check value type to avoid implicit conversion.
        if value is not None and not isinstance(value, basestring):
            raise ValueError('Field "%s" must be str or unicode, not %s.' %
                             (self.name, type(value)))
        value = super(StringField, self).validate(value, obj=obj)
        if value is not None:
            if self.max_length is not None and len(value) > self.max_length:
                raise ValueError(
                    'String value of "%s" field is too long' % (self.name))
            if self.min_length is not None and len(value) < self.min_length:
                raise ValueError(
                    'String value of "%s" field is too short' % (self.name))
            if self.regex is not None and self.regex.match(value) is None:
                raise ValueError(
                    'String value of "%s" field did not match regex' %
                    (self.name))
            try:
                value = value.encode(self.encoding).decode(self.encoding)
            except:
                pass
        return value


class NumberField(Field):

    def __init__(self, min_value=None, max_value=None, *args, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(NumberField, self).__init__(*args, **kwargs)

    def validate(self, value, obj=None):
        value = super(NumberField, self).validate(value, obj=obj)
        if value is not None:
            if self.min_value is not None and value < self.min_value:
                raise ValueError(
                    'Value of "%s" field cannot be less than %s' %
                    (self.name, self.min_value))
            if self.max_value is not None and value > self.max_value:
                raise ValueError(
                    'Value of "%s" field cannot be greater than %s' %
                    (self.name, self.max_value))
        return value


class IntegerField(NumberField):
    '''
    Details about long/int convertion can be found here:
    http://www.python.org/dev/peps/pep-0237/
    Note: str with number will be converted in base validate();
    So I think it's wrong, but it's here for back compatibility.
    '''
    field_type = int


class LongField(NumberField):

    field_type = long


class FloatField(NumberField):

    field_type = float


class BooleanField(Field):

    def is_empty(self, value):
        return value is None

    def validate(self, value, obj=None):
        value = super(BooleanField, self).validate(value, obj)
        if value is not None:
            if value not in (0, 1, False, True, 'off', 'on'):
                raise ValueError('Field "%s" must be a bool, not %s.' %
                                 (self.name, type(value)))
            value = bool(value) and value != 'off'
        return value


class DateTimeField(Field):

    field_type = datetime

    @staticmethod
    def utcnow():
        return datetime.now(tz=tz.tzutc())

    @staticmethod
    def utc_to_tz(utcvalue, totz=None):
        if totz is None:
            totz = tz.tzlocal()
        return utcvalue.astimezone(totz)

    def __init__(self, *args, **kwargs):
        self.auto_now = kwargs.pop('auto_now', False)
        self.auto_created = kwargs.pop('auto_created', False)
        assert not (self.auto_now and self.auto_created), 'auto mode set improperly'
        super(DateTimeField, self).__init__(*args, **kwargs)

    def __get__(self, obj, objtype=None):
        value = super(DateTimeField, self).__get__(obj, objtype)
        if self.auto_now:
            value = self.utcnow()
        elif value is None and self.auto_created:
            value = self.utcnow()
            self.__set__(obj, value)
        return value


class ListField(Field):
    '''
    TODO: List of embedded docs should work with references, rather than
    collections. Also syntacs like `x.y.append(z)` doesn't supported yet,
    and z will not be converted to `item_type`.
    '''
    field_type = list

    def __init__(self, item_type=None, *args, **kwargs):
        if item_type and not isinstance(item_type, type):
            raise TypeError('Item type should be a type object')
        self.item_type = item_type
        super(ListField, self).__init__(*args, **kwargs)

    def is_empty(self, value):
        return value is None  # empty list is valid value for required field

    def validate(self, value, obj=None):
        value = super(ListField, self).validate(value, obj)
        if value is not None and self.item_type is not None:
            value = [self.validate_item(v, obj) for v in value]
        return value

    def validate_item(self, item, obj=None):
        try:
            return _ensure_parent(_ensure_type(item, self.item_type), obj)
        except (TypeError, ValueError):
            raise ValueError('List item for "%s" field must be %s, not %s.' %
                             (self.name, self.item_type, type(item)))


class EmbeddedDocumentField(Field):

    field_type = dict  # usually document is pure dict

    def __init__(self, document_type, *args, **kwargs):
        from collection import Collection
        if not issubclass(document_type, Collection):
            raise TypeError(
                'Document type should be a Collection type, not %s.' %
                (document_type,))
        self.document_type = document_type
        super(EmbeddedDocumentField, self).__init__(*args, **kwargs)

    def validate(self, value, obj=None):
        value = super(EmbeddedDocumentField, self).validate(value, obj)
        if value is not None:
            document_type = self.document_type
            value = _ensure_type(value, document_type, document_type.create)
            value = _ensure_parent(value, obj)
        return value

    def __get__(self, obj, objtype=None):
        #TODO: required some magic to get document value instead field
        value = super(EmbeddedDocumentField, self).__get__(obj, objtype=objtype)
        return value

EmbeddedDocument = EmbeddedDocumentField


class DictField(Field):

    field_type = dict

    def __init__(self, item_type=None, *args, **kwargs):
        self.item_type = item_type
        super(DictField, self).__init__(*args, **kwargs)

    def validate(self, value, obj=None):
        value = super(DictField, self).validate(value, obj)
        if hasattr(value, 'iteritems') and self.item_type is not None:
            for key, item in value.iteritems():
                if not isinstance(item, self.item_type):
                    # Note that we work with same dict to avoid reference copy
                    value[key] = self.validate_item(key, item, obj)
        return value

    def validate_item(self, key, item, obj=None):
        try:
            return _ensure_parent(_ensure_type(item, self.item_type), obj)
        except (TypeError, ValueError):
            raise ValueError('Dict value for "%s" field must be %s, not %s.' %
                             (self.name, self.item_type, type(item)))

ObjectField = DictField


class ObjectIdField(Field):

    field_type = ObjectId
