from pprint import pprint, pformat
from couchbase.exceptions import KeyExistsError
from django.db.models.expressions import Col
import django.db

from django.db.models.sql import compiler
from django.db.models.sql.datastructures import EmptyResultSet
from django.db.models.sql.where import EmptyWhere, WhereNode

try:
    from django.db.models.sql.compiler import SQLDateCompiler as DateCompiler
except ImportError:
    class DateCompiler(object):
        pass
try:
    from django.db.models.sql.compiler import SQLDateTimeCompiler as DateTimeCompiler
except ImportError:
    class DateTimeCompiler(object):
        pass

from couchbase.n1ql import N1QLQuery, CONSISTENCY_REQUEST

from .utils import n1ql_escape, DocID
from .operators import Operators, Transforms
from .dbapi import IntegrityError, NotSupportedError


class Placeholders(object):
    def __init__(self):
        self.index = 1
        self.values = []

    def add(self, value):
        self.values.append(value)
        self.index += 1

    def indexstr(self):
        return '${0}'.format(self.index)

    def add_subquery_placeholders(self, other):
        """
        Merge placeholders with that of the subquery
        :param other: The other Placeholders object
        :return:
        """
        assert not self.values
        self.index = other.index
        self.values = other.values[::]


TYPEFIELD = '__CBTP'
BUCKET_PLACEHOLDER = '__BUCKET__'


def _ensure_json(val):
    import json
    try:
        json.dumps(val)
    except TypeError:
        pprint(val)
        raise

def _quote_fields(alias, field):
    return '{0}.{1}'.format(n1ql_escape(alias), n1ql_escape(field))


class SelectCommand(object):
    def __init__(self, connection, query, keys_only=False, is_aggregate=False):
        """
        Create a SELECT command
        :param query: The query
        :param bucket: The couchbase Bucket object
        :param keys_only: Whether to only return the ID initially
        :return:
        """
        self.top_query = query
        self.connection = connection
        self._keys_only = keys_only
        self.params = Placeholders()
        self.statement = []  # List of tokens to join when querying
        self._where = []
        self.is_count = True if query.annotations else False
        self.distinct = []
        self.aggregate_only = is_aggregate

        # Set to True if we have an actual WHERE clause which is not a PK lookup
        self._nopk_where = False

        # Used for updates, the name of the PK column
        self.pk_col_name = None

        # This is for backend-side aliases which have no equivalent in the actual
        # query. May be used for aggregates and others
        self.anon_alias_ix = 0

        self.is_pk_lookup = False  # Is a simple PK lookup (so we can do get/multi-get)
        self.pk_values = set()  # If PK lookup, how many PKs to select..

        # A list of (name, field) for each item added.. This appears in the order that
        # Django expects with respect to "rows".
        self.queried_fields = []
        self.unsupported_query_message = ""

        self.statement = self.process_query(
            self.top_query, is_aggregate=is_aggregate, keys_only=keys_only)

    def process_query(self, query, is_aggregate=False, keys_only=False):
        """
        Processes a Query object
        :param query: The query object to process
        :param is_aggregate: Whether the current query is an aggregate-only query
        :return: The query string
        """
        qstr = []
        where_list = []

        qstr.append('SELECT')
        qstr.append(self.get_fields(query, keys_only))
        qstr.append('FROM')
        if hasattr(query, 'subquery') and query.subquery:
            assert isinstance(query.subquery, SelectCommand)
            pprint(vars(query))
            pprint(vars(query.subquery))
            self.params.add_subquery_placeholders(query.subquery.params)
            qstr += ['('] + query.subquery.statement + [')', 'subquery']
        else:
            qstr.append(self.get_from(query, where_list=where_list))

        extra_where = self._get_where(query)
        if extra_where:
            where_list.append(extra_where)

        if where_list:
            qstr.append('WHERE')
            qstr.append(' AND '.join(where_list))

        order = self._get_ordering(query)
        if order:
            qstr.append('ORDER BY')
            qstr.append(','.join(order))
        if query.low_mark:
            qstr.append('OFFSET ' + str(query.low_mark))
        if query.high_mark:
            limit = query.high_mark - query.low_mark
            qstr.append('LIMIT ' + str(limit))

        return qstr

    def _get_ordering(self, query):
        q = query
        if not q.default_ordering:
            result = q.order_by
        else:
            result = q.order_by or q.get_meta().ordering

        if q.extra_order_by:
            all_fields = q.get_meta.get_all_field_names()
            new_ordering = []
            for col in q.extra_order_by:
                if col in q.extra_select:
                    if q.extra_select[col][0] in all_fields:
                        new_ordering.append(q.extra_select[col][0])
                    else:
                        pass
                else:
                    for col in all_fields:
                        new_ordering.append(col)

            result = tuple(new_ordering)

        if result:
            ordering = []
            for name in result:
                if name == '?':
                    ordering.append('RANDOM()')

                elif isinstance(name, int):
                    if name < 0:
                        direction = 'DESC'
                        name = -name
                    else:
                        direction = 'ASC'

                    # ORDER BY (int) is 1-based. Subtract one for lookup
                    name = self.queried_fields[name-1][0]
                    ordering.append(name + ' ' + direction)

                elif '__' not in name:
                    mm = q.model._meta
                    if name.startswith('-'):
                        name = name[1:]
                        direction = 'DESC'
                    else:
                        direction = 'ASC'

                    if name == 'pk':
                        field = mm.pk
                    else:
                        field = mm.get_field(name)

                    if field.primary_key:
                        # Determine the alias..
                        order_str = 'META({}).id'.format(n1ql_escape(BUCKET_PLACEHOLDER))
                    else:
                        order_str = n1ql_escape(field.column)

                    ordering.append(order_str + ' ' + direction)

            result = ordering

        return result

    def _maybe_add_pk_only_lookup(self, parent, child, rhs_value):
        if self._nopk_where:
            # Already invalidated
            return

        if child.lhs.target != self.query.get_meta().pk:
            # This is not a PK field
            self._nopk_where = True
            return

        if parent.connector == 'NOT':
            # The query wishes to exclude a given item
            self._nopk_where = True
            return

        if child.lookup_name not in ('exact', 'contains'):
            # This is not a direct query
            self._nopk_where = True
            return

        if child.lhs.target == self.query.get_meta().pk and parent.connector != 'NOT':
            self.is_pk_lookup = True
            if isinstance(rhs_value, (tuple, list)):
                self.pk_values.update(rhs_value)
            else:
                self.pk_values.add(rhs_value)

    def _process_where_node(self, parent, query):
        """
        Process a single WHERE node, possibly recursing.
        :param parent: The parent 'WHERE' clause
        :return:
        """
        if isinstance(parent, EmptyWhere):
            raise EmptyResultSet()

        where = []

        for child in parent.children:
            if isinstance(child, WhereNode):
                where.append(self._process_where_node(child, query))
                continue

            # Field as represented in the query
            query_field = child.lhs.output_field
            # Field of the DB table for the model
            real_field = child.lhs.target
            # Origin field (if this is a foreign field)
            origin_field = real_field.related_field if real_field.rel else real_field

            was_list = isinstance(child.rhs, (list, tuple))
            rhs_value = origin_field.get_db_prep_lookup(child.lookup_name, child.rhs, self.connection, prepared=True)
            if not was_list and rhs_value != []:
                rhs_value = rhs_value[0]

            if origin_field.primary_key:
                if real_field.rel:
                    # If we're a related field, use the foreign table,
                    # but also don't use META(id), since it's actually embedded
                    lhs_table = real_field.related_model._meta.db_table
                    lhs = real_field.column
                else:
                    lhs_table = query.alias_map[child.lhs.alias].table_name
                    lhs = 'META({}).id'.format(n1ql_escape(BUCKET_PLACEHOLDER))

                if real_field.get_internal_type() in ('IntegerField', 'AutoField'):
                    # This could be cast as a string, so cast it back as an int
                    castfn = int
                else:
                    castfn = lambda x_: x_

                if isinstance(rhs_value, (list, tuple)):
                    rhs_value = [DocID.encode(lhs_table, castfn(x)) for x in rhs_value]
                else:
                    rhs_value = DocID.encode(lhs_table, castfn(rhs_value))

            else:
                lhs = n1ql_escape(query_field.column)
                real_field = query_field

            placeholder = self.params.indexstr()
            rhs_value, criteria = Operators.convert(rhs_value, lhs, placeholder, child.lookup_name, real_field)
            self.params.add(rhs_value)

            where.append(' '.join(criteria))

        if where:
            connector = ' ' + parent.connector + ' '
            neg = 'NOT ' if parent.negated else ''
            return '(' + neg + (connector.join(where)) + ')'
        else:
            return None

    def _get_where(self, query):
        # Needed for empty_qs tests in basic
        return self._process_where_node(query.where, query)

    def _gen_alias(self):
        """
        Generate a unique alias for fields which don't have columns natively
        :return: A new unique alias
        """
        ss = '__Alias{0}'.format(self.anon_alias_ix)
        self.anon_alias_ix += 1
        return ss

    def handle_extra_select(self, query):
        # Handles extra select. For N1QL, the extra select must appear at the END
        # of the query, but for django they must appear at the beginning of the row.
        columns_str = []
        extra_field_info = []
        for alias, col in query.extra_select.items():
            columns_str.append('({src}) AS {dst}'.format(src=col[0], dst=n1ql_escape(alias)))
            extra_field_info.append((alias, None))

        if extra_field_info:
            self.queried_fields = extra_field_info + self.queried_fields
        return columns_str

    def get_fields(self, query, keys_only=False):
        """
        Get the fields to select, this returns the SELECT -> XXX <- part

        This will also popular the 'fieldstrs' field

        :return: The list of fields to query, properly quoted, as a string
        """

        # Field names to place back into the database
        q = query
        pk_field = q.get_meta().pk
        opts = q.get_meta()
        self.pk_col_name = pk_field.column

        if q.distinct_fields:
            raise Exception("Can't handle distinct_fields yet")

        columns_str = []

        fields = []
        if not self.aggregate_only:
            if q.select:
                fields = q.select
            elif q.default_cols:
                fields = [Col('_dummy_', x, x) for x in opts.fields]

        # pprint(vars(q))
        for col in fields:
            field = col.output_field
            column = field.column

            # Get the document field to select.
            if column == pk_field.column:
                sel_field = 'META({}).id'.format(n1ql_escape(BUCKET_PLACEHOLDER))
            elif keys_only:
                continue
            else:
                sel_field = n1ql_escape(column)

            # See if there's a lookup type.
            if hasattr(col, 'lookup_type'):
                # pprint(vars(q))
                col_alias = self._gen_alias()
                selstr, convfld = Transforms.transform(col.lookup_type, sel_field)
                if q.distinct:
                    self._nopk_where = True
                    selstr = 'DISTINCT({0})'.format(selstr)
                selstr += ' AS ' + col_alias
                columns_str.append(selstr)
                self.queried_fields.append((col_alias, convfld))
            else:
                if column == pk_field.column:
                    columns_str.append(sel_field + ' AS ' + column)
                else:
                    columns_str.append(sel_field)

                self.queried_fields.append((column, field))

        for alias, annotation in q.annotation_select.items():
            colspec = annotation.input_field.value

            if not alias:
                alias = self._gen_alias()

            fn = annotation.function
            agstr = '{0}({1}) AS {2}'.format(fn, colspec, n1ql_escape(alias))
            self.queried_fields.append((alias, None))
            columns_str.append(agstr)

        columns_str += self.handle_extra_select(query)
        return ','.join(columns_str)

    def get_from(self, query, where_list):
        model = query.model
        table_name = model._meta.db_table
        where_list.append('({}=="{}")'.format(TYPEFIELD, table_name))
        return BUCKET_PLACEHOLDER

    def dict_to_row(self, obj):
        rv = []
        for alias, field in self.queried_fields:
            try:
                if field is None:
                    rv.append(obj[alias])
                else:
                    rv.append(self.connection.ops.convert_values(obj[alias], field))
            except KeyError:
                if field and field.null:
                    # NULL values allowed
                    rv.append(None)
                else:
                    print "Missing key!", alias
                    print "Field", field
                    pprint(obj)
                    raise

        return rv

    def _execute_n1ql(self, bucket):
        s = ' '.join(self.statement).replace(BUCKET_PLACEHOLDER, bucket.bucket)

        print 'QUERY:', s
        print 'PARAMS:', self.params.values

        nq = N1QLQuery(s, *self.params.values)
        nq.consistency = CONSISTENCY_REQUEST
        # nq.consistent_with_all(bucket)

        # Bug here, PYCBC-290, if we return the iterator
        return bucket.n1ql_query(nq)

    def _execute_kv(self, bucket):
        print 'USING KV. Query:', self.statement
        print 'PARAMS:', self.params.values

        results = bucket.get_multi(self.pk_values, quiet=True)
        docs = []
        for res in results.values():
            if not res.success:
                continue

            if self.pk_col_name:
                res.value[self.pk_col_name] = res.key
            docs.append(res.value)

        return docs

    def execute(self, bucket):
        if self.unsupported_query_message:
            raise NotSupportedError(self.unsupported_query_message)

        # if self._nopk_where or not self.pk_values:
        if True:
            return self._execute_n1ql(bucket)
        else:
            return self._execute_kv(bucket)


class InsertCommand(object):
    def __init__(self, connection, model):
        self.model = model
        self.connection = connection
        self._executed = False

    def get_params(self, objs, fields):
        to_insert = {}
        table = self.model._meta.db_table

        for obj in objs:
            docid = None
            doc = {}

            for field in fields:
                # Process field
                value = field.get_db_prep_save(field.pre_save(obj, True), self.connection)
                if value is None and field.get_internal_type() != 'AutoField':
                    if field.has_default():
                        value = field.get_default()
                    if value is None and not field.null:
                        raise django.db.IntegrityError(
                            'None found for field {} but field is non-nullable'.format(field))

                # FIXME: Can an AutoField have a default/NULL value?
                if value is None and field.get_internal_type() == 'AutoField':
                    continue

                # If it's not None, convert it finally..
                if value is not None:
                    value = self.connection.ops.value_for_db(value, field)

                # Ensure we don't have a NULL PK
                assert not (value is None and field.primary_key)

                if field.get_internal_type() == 'ForeignKey' and value is not None:
                    tgt_table = field.rel.to._meta.db_table
                    value = DocID.encode(tgt_table, value)

                # Only for string DocIDs
                if field.primary_key and value is not None:
                    docid = DocID.encode(table, value)
                elif not field.primary_key:
                    doc[field.column] = value

            if docid is None:
                docid = DocID.generate(table)

            assert isinstance(docid, basestring)

            # Insert the TYPE field
            doc[TYPEFIELD] = table
            to_insert[docid] = doc

        # print 'Inserting:', pformat(to_insert)
        return to_insert

    def execute(self, bucket, to_insert):
        if self._executed:
            raise Exception('Already executed!')

        self._executed = True
        # Gets the bucket and the params. It's simple!
        try:
            return bucket.insert_multi(to_insert)
        except KeyExistsError as e:
            raise IntegrityError(e)


class UpdateCommand(object):
    def __init__(self, connection, query):
        self.query = query
        self.select = SelectCommand(connection, query, keys_only=True)
        self.connection = connection

    def execute(self, bucket):
        rows = [x for x in self.select.execute(bucket)]
        # pprint(rows)

        ids = [x[self.select.pk_col_name] for x in rows]
        if not ids:
            return 0

        docs = bucket.get_multi(ids)
        # pprint(vars(self.query))
        to_update = {}

        for res in docs.values():
            merge = {}
            for field, model, value in self.query.values:
                if hasattr(value, 'prepare_database_save'):
                    value = value.prepare_database_save(field)
                else:
                    value = field.get_db_prep_save(value, self.connection)

                print "UPDATE: ", field, model, value, field.db_type(self.connection)

                # Get the actual destination name
                if field.get_internal_type() == 'ForeignKey':
                    if value is None:
                        assert field.null
                        merge[field.column] = None
                        continue

                    # if not isinstance(value, (basestring, int, long)):
                    #     pprint(value)
                    #     pprint(vars(field))

                    value = DocID.encode(self.query.model._meta.db_table, value)
                else:
                    value = self.connection.ops.value_for_db(value, field)

                merge[field.column] = value

            doc = res.value
            doc.update(merge)
            to_update[res.key] = doc

        bucket.replace_multi(to_update)
        return len(docs)


class DeleteCommand(object):
    def __init__(self, connection, query):
        self.select = SelectCommand(connection, query, keys_only=True)
        self.connection = connection

    def execute(self, bucket):
        rows = [x for x in self.select.execute(bucket)]
        ids = [x[self.select.pk_col_name] for x in rows]
        if not ids:
            return 0

        bucket.remove_multi(ids)
        return len(ids)


class FlushCommand(object):
    def __init__(self, tables):
        self.tables = tables

    def execute(self, bucket):
        if self.tables:
            params = self.tables
            qstr = 'SELECT META({bucket}).id AS id FROM {bucket} WHERE {typefield} IN $1'
            qstr = qstr.format(bucket=bucket.bucket, typefield=TYPEFIELD)
        else:
            qstr = 'SELECT META(`{0}`).id AS id FROM `{0}`'.format(bucket.bucket)
            params = []

        nq = N1QLQuery(qstr, *params)
        nq.consistency = CONSISTENCY_REQUEST
        for row in bucket.n1ql_query(nq):
            bucket.remove(row['id'])

        # else:
        #     bucket.flush()


class CreateIndexCommand(object):
    def __init__(self, ix_specs):
        specs = {}
        pprint(ix_specs)
        for name, cols in ix_specs:
            s = 'CREATE INDEX {} ON {}({}) USING gsi'
            s = s.format(
                n1ql_escape(name),
                n1ql_escape(BUCKET_PLACEHOLDER),
                ','.join(n1ql_escape(x) for x in cols))
            specs[name] = s
        self.specs = specs

    def execute(self, bucket):
        # Create the filter SQL
        print "Executing SQL..."
        s = 'SELECT `name` FROM system:indexes WHERE `keyspace_id`="{}"'
        q = N1QLQuery(s.format(bucket.bucket))

        try:
            ids = [x['name'] for x in bucket.n1ql_query(q)]
        except Exception as e:
            pprint(e)
            raise

        for name, stmt in self.specs.items():
            if name in ids:
                continue
            try:
                stmt = stmt.replace(BUCKET_PLACEHOLDER, bucket.bucket)
                print stmt
                res = bucket.n1ql_query(stmt).get_single_result()
                pprint(res)
                print "Created index..."
            except Exception as e:
                print e
                raise

class SQLCompiler(compiler.SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        self.pre_sql_setup()
        self.refcounts_before = self.query.alias_refcount.copy()
        return SelectCommand(self.connection, self.query, is_aggregate=self._cb_aggregate_only), None

    _cb_aggregate_only = False


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def __init__(self, *args, **kwargs):
        self.return_id = None
        super(SQLInsertCompiler, self).__init__(*args, **kwargs)

    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        self.pre_sql_setup()
        # pprint(vars(self.query))

        # Always pass down all the fields on an insert
        cmd = InsertCommand(self.connection, self.query.model)
        params = cmd.get_params(self.query.objs, self.query.fields)
        return [(cmd, params)]


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    def __init__(self, *args, **kwargs):
        super(SQLUpdateCompiler, self).__init__(*args, **kwargs)

    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        self.pre_sql_setup()
        return UpdateCommand(self.connection, self.query), []


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        return DeleteCommand(self.connection, self.query), []


class SQLDateTimeCompiler(DateTimeCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(SQLCompiler):
    _cb_aggregate_only = True
