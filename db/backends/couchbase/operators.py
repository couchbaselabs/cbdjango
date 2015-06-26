from datetime import datetime
from django.db.models import DateTimeField


class CustomOperator(object):
    @classmethod
    def get_constraint(cls, lhs, rhs):
        """
        Gets the constraint string for the given operator
        :param lhs: The lefthand side
        :param rhs: The righthand side
        :return: An iterable of tokens to concatenate with ' '
        """
        raise NotImplementedError()

    @classmethod
    def process_rhs(cls, rhs):
        """
        Perform any processing on the RHS so it can be suitable as a parameter
        :return:
        """
        return rhs

class LikeOperator(CustomOperator):
    @classmethod
    def get_constraint(cls, lhs, rhs):
        return lhs, 'LIKE', rhs

    @classmethod
    def process_rhs(cls, rhs):
        raise NotImplementedError()


class ContainsOperator(LikeOperator):
    @classmethod
    def process_rhs(cls, rhs):
        return '%{0}%'.format(rhs)


class StartsWithOperator(LikeOperator):
    @classmethod
    def process_rhs(cls, rhs):
        return '{0}%'.format(rhs)


class EndsWithOperator(LikeOperator):
    @classmethod
    def process_rhs(cls, rhs):
        return '%{0}'.format(rhs)


class DateParseOperator(CustomOperator):
    date_part = None
    adj = ''

    @classmethod
    def get_constraint(cls, lhs, rhs):
        return 'DATE_PART_STR({lhs}, "{part}"){adj}'.format(
            lhs=lhs, part=cls.date_part, adj=cls.adj), '=', rhs


class DayOfWeekOperator(DateParseOperator):
    date_part = 'iso_dow'
    adj = '+1'


def _mk_dateop(ss):
    class _Dateop(DateParseOperator):
        date_part = ss
    return _Dateop

OPERATOR_MAP = {
    'exact': lambda lhs, rhs: (lhs, '=', rhs),
    'gt': lambda lhs, rhs: (lhs, '>', rhs),
    'gte': lambda lhs, rhs: (lhs, '>=', rhs),
    'lt': lambda lhs, rhs: (lhs, '<', rhs),
    'lte': lambda lhs, rhs: (lhs, '<=', rhs),
    'in': lambda lhs, rhs: (lhs, 'IN', rhs),
    'year': _mk_dateop("year"),
    'month': _mk_dateop("month"),
    'day': _mk_dateop("day"),
    'week_day': DayOfWeekOperator,
    'contains': ContainsOperator,
    'startswith': StartsWithOperator,
    'endswith': EndsWithOperator
}


class Operators(object):
    @staticmethod
    def convert(rhs, lhs, placeholder, lookup):
        opfn = OPERATOR_MAP[lookup]
        if hasattr(opfn, 'get_constraint'):
            tokens = opfn.get_constraint(lhs, placeholder)
            rhs = opfn.process_rhs(rhs)
        else:
            tokens = opfn(lhs, placeholder)

        return rhs, tokens


DATE_MAPS = {
    'year': 'year',
    'month': 'month',
    'day': 'day',
    'week_day': 'iso_dow'
}

# FFS
class DateTransformField(DateTimeField):
    def __init__(self, mode):
        super(DateTransformField, self).__init__()
        self.__mode = mode

    def convert(self, obj):
        if self.__mode == 'year':
            return datetime(year=obj.year, month=1, day=1)
        elif self.__mode == 'month':
            return datetime(year=obj.year, month=obj.month, day=1)
        elif self.__mode == 'day':
            return datetime(year=obj.year, month=obj.month, day=obj.day)
        else:
            raise ValueError('Unknown conversion type!')


class Transforms(object):
    @staticmethod
    def transform(lookup, column):
        try:
            date_part = DATE_MAPS[lookup]
            return ('STR_TO_UTC(DATE_TRUNC_STR({0}, "{1}"))'.format(column, date_part),
                    DateTransformField(lookup))
        except KeyError:
            raise ValueError('Invalid lookup type: ' + lookup)
