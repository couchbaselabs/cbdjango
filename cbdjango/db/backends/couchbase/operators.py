from datetime import datetime, date
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

class LikeOperatorNC(CustomOperator):
    @classmethod
    def get_constraint(cls, lhs, rhs):
        return 'LOWER({})'.format(lhs), '=', rhs

    @classmethod
    def process_rhs(cls, rhs):
        return rhs.lower()


class RegexOperator(CustomOperator):
    @classmethod
    def get_constraint(cls, lhs, rhs):
        return ['REGEXP_CONTAINS( TOSTRING({}),{} )'.format(lhs, rhs)]


class RegexOperatorNC(CustomOperator):
    @classmethod
    def get_constraint(cls, lhs, rhs):
        return ['REGEXP_CONTAINS( LOWER(TOSTRING({})), LOWER({}) )'.format(lhs, rhs)]


class DateParseOperator(CustomOperator):
    date_part = None
    adj = ''

    @classmethod
    def get_constraint(cls, lhs, rhs):
        return 'DATE_PART_STR({lhs}, "{part}"){adj}'.format(
            lhs=lhs, part=cls.date_part, adj=cls.adj), '=', rhs

    @classmethod
    def process_rhs(cls, rhs):
        if isinstance(rhs, (basestring, int)):
            return rhs

        assert isinstance(rhs, (datetime, date))
        if cls.date_part == 'year':
            return rhs.year
        elif cls.date_part == 'month':
            return rhs.month
        elif cls.date_part == 'day':
            return rhs.day
        elif cls.date_part == 'iso_dow':
            return rhs.isoweekday()
        else:
            raise Exception('Unrecognized year lookup!')


class DayOfWeekOperator(DateParseOperator):
    date_part = 'iso_dow'
    adj = '+1'


class DateComparisonOperator(object):
    @classmethod
    def get_constraint(cls, lhs, placeholder, sym):
        return 'STR_TO_MILLIS({})'.format(lhs), sym, 'STR_TO_MILLIS({})'.format(placeholder)


def _mk_dateop(ss):
    class _Dateop(DateParseOperator):
        date_part = ss
    return _Dateop

# Simple arithmetic comparison fields
SIMPLE_CMP_MAP = {
    'exact': '=',
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<='
}

OPERATOR_MAP = {
    'iexact': LikeOperatorNC,
    'in': lambda lhs, rhs: (lhs, 'IN', rhs),
    'year': _mk_dateop("year"),
    'month': _mk_dateop("month"),
    'day': _mk_dateop("day"),
    'week_day': DayOfWeekOperator,
    'contains': ContainsOperator,
    'startswith': StartsWithOperator,
    'endswith': EndsWithOperator,
    'regex': RegexOperator,
    'iregex': RegexOperatorNC
}

class Operators(object):
    @staticmethod
    def convert(rhs, lhs, placeholder, lookup, field):
        """
        Convert a lookup into a set of N1QL tokens
        :param rhs: The value to compare to
        :param lhs: The column to compare
        :param placeholder: The placeholder representing the right hand value
        :param lookup: The lookup type
        :param field: The field associated with the left-hand side
        :return: A tuple of (rhs_value, tokens), where rhs_value is the VALUE to use
            for the placeholder. This may change if the rhs value needs to be modified
            (in Python).
        """
        if lookup in SIMPLE_CMP_MAP:
            nsym = SIMPLE_CMP_MAP[lookup]
            if field.get_internal_type() in ('DateField', 'DateTimeField'):
                tokens = DateComparisonOperator.get_constraint(lhs, placeholder, nsym)
            else:
                tokens = lhs, nsym, placeholder
        else:
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
