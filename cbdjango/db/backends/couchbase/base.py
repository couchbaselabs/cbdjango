from pprint import pprint

from django.db import DatabaseError
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.validation import BaseDatabaseValidation
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from django.utils.dateparse import parse_datetime, parse_date


from couchbase.bucket import Bucket
from couchbase.connstr import ConnectionString

import cbdjango.db.backends.couchbase.dbapi as Database
from .utils import n1ql_escape, DocID
from .compiler import SelectCommand, InsertCommand, UpdateCommand, FlushCommand,\
    DeleteCommand, CreateIndexCommand

from .operators import DateTransformField

class Connection(object):
    """ Dummy connection class """
    def __init__(self, wrapper, params, bucket):
        self.creation = wrapper.creation
        self.ops = wrapper.ops
        self.params = params
        self.queries = []
        self.wrapper = wrapper
        self.bucket = bucket

    def rollback(self):
        pass

    def commit(self):
        pass

    def close(self):
        pass


class Cursor(object):
    """ Dummy cursor class """
    def __init__(self, connection):
        self.connection = connection
        self.bucket = connection.bucket
        self._iter = None
        self._cmd = None
        self._results = None
        self._lastid = None
        self.rowcount = 0

    def execute(self, sql, *args):
        self._cmd = sql
        if isinstance(sql, SelectCommand):
            self._iter = iter(sql.execute(self.bucket))
            self._results = None
        elif isinstance(sql, InsertCommand):
            self._results = sql.execute(self.bucket, *args).values()
            self._iter = iter(self._results)
        elif isinstance(sql, UpdateCommand):
            self.rowcount = sql.execute(self.bucket)
        elif isinstance(sql, FlushCommand):
            sql.execute(self.bucket)
        elif isinstance(sql, DeleteCommand):
            self.rowcount = sql.execute(self.bucket)
        elif isinstance(sql, CreateIndexCommand):
            sql.execute(self.bucket)

    # def next(self):
    #     return self._fetchone()

    def _fetchone(self, delete_flag=False):
        rv = next(self._iter)
        if self._results:
            self._lastid = rv.key

        else:
            # Results is a column. Unpack the query
            rv = self._cmd.dict_to_row(rv)

        print "Returning:", rv
        return rv

    def fetchone(self, delete_flag=False):
        try:
            return self._fetchone(delete_flag)
        except StopIteration:
            return None

    def fetchmany(self, size, delete_flag=False):
        rv = []
        for x in range(size):
            row = self.fetchone(delete_flag)
            if not row:
                break
            rv.append(row)

        return rv

    @property
    def lastrowid(self):
        if self._results:
            rv = DocID.decode(self._results[-1].key).to_int()
            assert rv is not None
            return rv

        raise Exception('Requested last Row ID, but is -1')
        return -1

    def __iter__(self):
        return self

    def close(self):
        pass


class DatabaseCreation(BaseDatabaseCreation):
    data_types = {
        'AutoField':                  'key',
        'RelatedAutoField':           'key',
        'ForeignKey':                 'key',
        'OneToOneField':              'key',
        'ManyToManyField':            'key',
        'BigIntegerField':            'long',
        'BooleanField':               'bool',
        'CharField':                  'string',
        'CommaSeparatedIntegerField': 'string',
        'DateField':                  'date',
        'DateTimeField':              'datetime',
        'DecimalField':               'decimal',
        'EmailField':                 'string',
        'FileField':                  'string',
        'FilePathField':              'string',
        'FloatField':                 'float',
        'ImageField':                 'string',
        'IntegerField':               'integer',
        'IPAddressField':             'string',
        'NullBooleanField':           'bool',
        'PositiveIntegerField':       'integer',
        'PositiveSmallIntegerField':  'integer',
        'SlugField':                  'string',
        'SmallIntegerField':          'integer',
        'TimeField':                  'time',
        'URLField':                   'string',
        'TextField':                  'text',
        'XMLField':                   'text',
    }

    def sql_create_model(self, model, *args, **kwargs):
        return [], {}

    def sql_for_pending_references(self, model, *args, **kwargs):
        return []

    def sql_indexes_for_fields(self, model, fields, style):
        cols = []
        for field in fields:
            if field.primary_key:
                continue
            cols.append(field.column)

        ix_name = 'idx_' + '_'.join(cols)
        return [[ix_name, cols]]

    def sql_indexes_for_model(self, model, *args, **kwargs):
        s = super(self.__class__, self).sql_indexes_for_model(model, *args, **kwargs)
        # if s:
        #     return [CreateIndexCommand(s)]
        return []

    def create_test_db(self, verbosity=1, autoclobber=False, serialize=True, keepdb=False):
        return 'test_'


class DatabaseFeatures(BaseDatabaseFeatures):
    empty_fetchmany_value = []
    supports_transactions = False
    can_return_id_from_insert = True
    supports_select_related = False
    autocommits_when_autocommit_is_off = True
    uses_savepoints = False
    allows_auto_pk_0 = True  # Anything is OK

def coerce_unicode(value):
    if isinstance(value, str):
        try:
            value = value.decode('utf-8')
        except UnicodeDecodeError:
            # This must be a Django databaseerror, because the exception happens too
            # early before Django's exception wrapping can take effect (e.g. it happens on SQL
            # construction, not on execution.
            raise DatabaseError("Bytestring is not encoded in utf-8")

    # The SDK raises BadValueError for unicode sub-classes like SafeText.
    return unicode(value)


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = 'cbdjango.db.backends.couchbase.compiler'

    def quote_name(self, name):
        return n1ql_escape(name)

    def fetch_returned_insert_id(self, cursor):
        return cursor.lastrowid

    def convert_values(self, value, field):
        # Normalize the DocID back
        if field.primary_key:
            value = DocID.decode(value)
            if hasattr(field, 'rel') and field.rel:
                field = field.rel.to

            if field.get_internal_type() in ('AutoField', 'IntegerField'):
                return value.to_int()
            elif field.get_internal_type() in ('CharField', 'TextField'):
                return value.to_string()
            else:
                raise Exception('Unknown internal type ' + field.get_internal_type())

        if field.get_internal_type() == 'AutoField' and not isinstance(value, DocID):
            return DocID.decode(value).to_int()
        elif field.get_internal_type() == 'DateTimeField':
            # print "Converting DateTimeField..", value
            value = parse_datetime(value)
            if isinstance(field, DateTransformField):
                value = field.convert(value)
            return value
        elif field.get_internal_type() == 'DateField':
            return parse_date(value)
        else:
            return value

    def sql_flush(self, style, tables, seqs, allow_cascade=False):
        return [FlushCommand(tables)]

    def value_for_db(self, value, field):
        if value is None:
            return None

        db_type = field.db_type(self.connection)

        if db_type == 'string' or db_type == 'text':
            value = coerce_unicode(value)
        elif db_type == 'bytes':
            # Store BlobField, DictField and EmbeddedModelField values as Blobs.
            value = bytes(value)
        elif db_type == 'decimal':
            value = self.value_to_db_decimal(value, field.max_digits, field.decimal_places)
        elif db_type in ('list', 'set'):
            if hasattr(value, "__len__") and not value:
                value = None  # Convert empty lists to None
            elif hasattr(value, "__iter__"):
                # Convert sets to lists
                value = list(value)

        elif db_type == 'datetime':
            value = self.value_to_db_datetime(value)
        elif db_type == 'date':
            value = self.value_to_db_date(value)

        return value


class DatabaseClient(BaseDatabaseClient):
    pass


class DatabaseValidation(BaseDatabaseValidation):
    pass


class DatabaseIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        cb = self.connection.get_new_connection({})
        bucket = cb.bucket
        qstr = 'SELECT DISTINCT __CBTP FROM {0}'.format(n1ql_escape(bucket.bucket))
        return [TableInfo(x, "t") for x in bucket.n1ql_query(qstr)]


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    def column_sql(self, model, field):
        return "", {}

    def create_model(self, model):
        """ Don't do anything when creating tables """
        pass

    def alter_unique_together(self, *args, **kwargs):
        pass

    def alter_field(self, from_model, from_field, to_field):
        pass

    def remove_field(self, from_model, field):
        pass


class DatabaseWrapper(BaseDatabaseWrapper):
    Database = Database

    def __init__(self, *args, **kwds):
        super(DatabaseWrapper, self).__init__(*args, **kwds)
        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.validation = DatabaseValidation(self)
        self.introspection = DatabaseIntrospection(self)
        self._buckets = {}

    def schema_editor(self, *args, **kwargs):
        return DatabaseSchemaEditor(self, *args, **kwargs)

    def get_connection_params(self):
        return {}

    def get_new_connection(self, conn_params):
        name = self.settings_dict['NAME']
        if not name:
            name = 'default'
        try:
            bucket = self._buckets[name]
        except KeyError:
            print "Connecting to bucket", name
            cstr = ConnectionString.parse(self.settings_dict['CONNECTION_STRING'])
            cstr.options['fetch_mutation_tokens'] = '1'

            cstr.bucket = name
            bucket = Bucket(str(cstr))
            self._buckets[name] = bucket

        return Connection(self, {}, bucket)

    def _set_autocommit(self, autocommit):
        self.autocommit = autocommit

    def _start_transaction_under_autocommit(self):
        pass

    def init_connection_state(self):
        pass

    def create_cursor(self):
        if not self.connection:
            self.connection = self.get_new_connection(self.settings_dict)
        return Cursor(self.connection)

    def is_usable(self):
        return True

