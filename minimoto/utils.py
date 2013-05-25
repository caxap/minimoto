import re
import math


class Object(object):
    pass


class RetryError(Exception):
    pass


#TODO (mkamenkov): should be `yield Optimistic(action, args, kwargs, repeats=3)`
def optimistic(action, args=None, kwargs=None, repeats=3,
               retry_on_error=True, callback=None):
    '''
    Optimistic way to execute db updates. If some condition is broken
    client code can raise `RetryError` to retry call. Also if retry_on_error
    is set to True action will be retried.
    '''
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}
    # work flow for `gen.Task` callbacks chain
    if callback is not None:
        kwargs.setdefault('callback', callback)
    for attempt in range(1, repeats + 1):
        try:
            action(*args, **kwargs)
            break
        except RetryError:
            if attempt == repeats:
                raise
        except Exception:
            if not retry_on_error or attempt == repeats:
                raise


def match_exact(query, delim=None, base='.*(%(pattern)s).*', options='i', min_length=3):
    '''
    Usage:
    >>> match_exact('Vasia Pupkin')
    {'$options': 'i', '$regex': '.*(Vasia|Pupkin).*'}

    Returns `None` if bad query.
    '''
    parts = set([re.escape(x) for x in query.split(delim) if len(x) >= min_length])
    if parts:
        regex = base % {'pattern': '|'.join(parts)}
        rspec = {'$regex': regex}
        if options:
            rspec['$options'] = options
        return rspec


def maybe_multi(value, safe=True):
    '''
    Usage:
    >>> maybe_multi(1)
    1
    >>> maybe_multi([1, ])
    1
    >>> maybe_multi([1,2, ])
    {'$in': [1, 2]}
    '''
    if value is None and safe:
        return []
    if isinstance(value, (list, tuple, set)):
        if len(value) == 1:
            return value[0]
        return {'$in': list(value)}
    return value


def model_fields(include=None, exclude=None):
    '''
    Usage:
    >>> model_fields('foo,baz','bar')
    {'baz': 1, 'foo': 1, 'bar': 0}
    >>> model_fields('bar', ['foo', 'baz'])
    {'bar': 1, 'foo': 0, 'baz': 0}
    '''
    include = dict([(x, 1) for x in _to_list(include)])
    exclude = dict([(x, 0) for x in _to_list(exclude)])
    return dict(exclude, **include)


def _to_list(value, delim=','):
    if not value:
        return []
    if isinstance(value, basestring) and delim in value:
        return value.split(delim)
    if not isinstance(value, (list, tuple, set)):
        return [value, ]
    return list(value)


class Paginator(object):
    """
    An object responsible for pagination processing. Pages counting from one.
    """
    def __init__(self, page, per_page, total_count):
        self.page = page
        self.per_page = per_page
        self.total_count = total_count

    @property
    def page_count(self):
        return int(math.ceil(float(self.total_count) / self.per_page))

    @property
    def current_page(self):
        if self.page <= 0:
            return 1
        if self.page > self.page_count:
            return self.page_count
        return self.page

    @property
    def skip(self):
        return int((self.current_page - 1) * self.per_page)

    @property
    def limit(self):
        return self.per_page

    @property
    def last(self):
        return self.current_page == self.page_count

    @property
    def first(self):
        return self.current_page == 1

    @property
    def page_range(self):
        return range(1, self.page_count + 1)

    def iterate_pages(self):
        range_len = 10
        half_range = int(math.ceil(float(range_len) / 2))

        if self.page_count < range_len:
            return self.page_range
        elif self.current_page < half_range:
            return self.page_range[:range_len]
        elif self.page_count - self.current_page < half_range:
            return self.page_range[-range_len:]
        else:
            return self.page_range[self.current_page - half_range:self.current_page + half_range - 1]

    def paginate(self, query):
        return query.skip(self.skip).limit(self.limit)

    def __call__(self, query):
        return self.paginate(query)


class Sorter(object):
    """
    An object responsible for sorting params processing
    """
    DIRECTION_ASC = 1
    DIRECTION_DESC = -1

    def __init__(self, **params):
        self._sort_params = params

    @property
    def sort_params(self):
        """
            returns a list of sorting params in format:
            (sort_field, direction)
        """
        return self._sort_params.items()

    @sort_params.deleter  # noqa
    def sort_params(self):
        del self._sort_params
        self._sort_params = {}

    def sort_param(self, field):
        return self._sort_params.get(field, None)

    def add_sort_param(self, sort_field, direction=DIRECTION_ASC):
        self._sort_params[sort_field] = direction

    def sort(self, query):
        sort_params = self.sort_params
        if sort_params:
            return query.sort(sort_params)
        return query

    @property
    def directions(self):
        return [direction for field, direction in self.sort_params]

    @property
    def fields(self):
        return [field for field, direction in self.sort_params]

    def __call__(self, query):
        return self.sort(query)


class Filter(object):
    def __init__(self):
        self._filter_params = dict()

    @property
    def filter_params(self):
        """
            returns a list of filtering params in format:
            (filter_field, filter_value)
        """
        return self._filter_params.items()

    @filter_params.deleter  # noqa
    def filter_params(self):
        del self._filter_params
        self._filter_params = {}

    def filter_param(self, field):
        return self._filter_params.get(field, None)

    def filter(self, query):
        filter_params = self.filter_params
        if filter_params:
            return query.filter(filter_params)
        return query

    @property
    def fields(self):
        return [field for field, direction in self.filter_params]
