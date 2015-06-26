from pprint import pprint, pformat

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


class Placeholders(object):
    def __init__(self):
        self.index = 1
        self.values = []

    def add(self, value):
        self.values.append(value)
        self.index += 1

    def indexstr(self):
        return '${0}'.format(self.index)


TYPEFIELD = '__CBTP'
BUCKET_PLACEHOLDER = '__BUCKET__'

def _quote_fields(alias, field):
    return '{0}.{1}'.format(n1ql_escape(alias), n1ql_escape(field))


class SelectCommand(object):
    def __init__(self, connection, query, keys_only=False):
        """
        Create a SELECT command
        :param query: The query
        :param bucket: The couchbase Bucket object
        :param keys_only: Whether to only return the ID initially
        :return:
        """
        self.query = query
        self.connection = connection
        self._keys_only = keys_only
        self._params = Placeholders()
        self._ss = []  # List of tokens to join when querying
        self._where = []
        self._cbtp_tables = set()
        self.is_count = query.aggregates
        self.distinct = []

        # This is for backend-side aliases which have no equivalent in the actual
        # query. May be used for aggregates and others
        self.anon_alias_ix = 0

        self.is_pk_lookup = False  # Is a simple PK lookup (so we can do get/multi-get)
        self.pk_values = set()  # If PK lookup, how many PKs to select..

        # A list of (name, field) for each item added..
        self.queried_fields = []

        self._ss.append('SELECT')
        self._ss.append(self.get_fields(keys_only))
        self._ss.append('FROM')
        self._ss.append(self.get_from())
        self._ss.append('WHERE')

        extra_where = self._get_where()
        if extra_where:
            self._where.append(extra_where)

        self._ss.append(' AND '.join(self._where))
        order = self._get_ordering()
        if order:
            self._ss.append('ORDER BY')
            self._ss.append(','.join(order))
        if self.query.low_mark:
            self._ss.append('OFFSET ' + str(self.query.low_mark))
        if self.query.high_mark:
            limit = self.query.high_mark - self.query.low_mark
            self._ss.append('LIMIT ' + str(limit))

    def _get_ordering(self):
        q = self.query
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

                elif not (isinstance(name, basestring) and '__' in name):
                    mm = q.model._meta
                    field = mm.get_field_by_name(name)[0]
                    ordering.append(_quote_fields(mm.db_table, field.column))

            result = ordering

        return result

    def _process_where_node(self, parent):
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
                where.append(self._process_where_node(child))
                continue

            rhs_value = child.rhs
            lhs_table = self.query.alias_map[child.lhs.alias].table_name

            # This is really an ID somewhere..
            if child.lhs.source.get_internal_type() == 'AutoField':
                if isinstance(rhs_value, (list, tuple)):
                    rhs_value = [DocID.encode(lhs_table, x) for x in rhs_value]
                else:
                    rhs_value = DocID.encode(lhs_table, rhs_value)

                lhs = 'META({}).id'.format(child.lhs.alias)
                if child.lhs.target == self.query.get_meta().pk:
                    self.is_pk_lookup = True
                    if isinstance(rhs_value, (tuple, list)):
                        self.pk_values.add(*rhs_value)
                    else:
                        self.pk_values.add(rhs_value)
                        # TODO: See if we can do a proper PK lookup. Doing this correctly
                        # would also involve checking if we're not part of an 'OR' clause,
                        # and that there aren't any other constraints (i.e. constraints
                        # placed by the user.. other than type constraints, of course)
            else:
                lhs = _quote_fields(child.lhs.alias, child.lhs.source.column)

            placeholder = self._params.indexstr()
            rhs_value, criteria = Operators.convert(rhs_value, lhs, placeholder, child.lookup_name)
            self._params.add(rhs_value)

            where.append(' '.join(criteria))

        if where:
            connector = ' ' + parent.connector + ' '
            return '(' + (connector.join(where)) + ')'
        else:
            return None

    def _get_where(self):
        # Needed for empty_qs tests in basic
        return self._process_where_node(self.query.where)

    def _select_id(self, table, name=None):
        """
        Gets the token used to select the ID of the row
        :param name: The name to use in the result, will be an 'AS' statement.
            If name is None, then only the column itself is used
        :return: A string for use with select
        """
        if not name:
            return 'META({0})'.format(n1ql_escape(table))
        else:
            return 'META({0}).id AS {1}'.format(n1ql_escape(table), n1ql_escape(name))

    def _gen_alias(self):
        """
        Generate a unique alias for fields which don't have columns natively
        :return: A new unique alias
        """
        ss = '__Alias{0}'.format(self.anon_alias_ix)
        self.anon_alias_ix += 1
        return ss

    def get_fields(self, keys_only=False):
        """
        Get the fields to select, this returns the SELECT -> XXX <- part

        This will also popular the 'fieldstrs' field

        :return: The list of fields to query, properly quoted, as a string
        """

        # Field names to place back into the database
        q = self.query
        pk_field = q.get_meta().pk
        opts = q.get_meta()

        if q.distinct_fields:
            raise Exception("Can't handle distinct_fields yet")

        columns_str = []
        for alias, col in q.extra_select.items():
            columns_str.append('({src}) AS {dst}'.format(src=col[0], dst=n1ql_escape(alias)))
            self.queried_fields.append((alias, None))

        # pprint(vars(q))
        if q.group_by is not None:
            pprint(q.group_by)
            raise Exception('GROUPING!!!')

        if q.select:
            for col, field in self.query.select:
                if field is None:
                    # This can happen for DateTime objects..
                    table_alias, column = col.col
                else:
                    table_alias, column = col

                # Several variables to declare here:
                # table_alias: The table name, as known to N1QL
                # sel_field: The actual string to use for SELECT
                # column: The column name (according ot django)
                # field: The actual Field object in the model

                # Get the document field to select.
                if column == pk_field.column:
                    sel_field = self._select_id(table_alias)
                elif keys_only:
                    continue
                else:
                    sel_field = _quote_fields(table_alias, column)

                # See if there's a lookup type.
                if hasattr(col, 'lookup_type'):
                    pprint(vars(q))
                    col_alias = self._gen_alias()
                    selstr, convfld = Transforms.transform(col.lookup_type, sel_field)
                    if q.distinct:
                        selstr = 'DISTINCT({0})'.format(selstr)
                    selstr += ' AS ' + col_alias
                    columns_str.append(selstr)
                    self.queried_fields.append((col_alias, convfld))
                else:
                    if column == pk_field.column:
                        # META(table_alias).id AS id
                        columns_str.append(sel_field + ' AS ' + column)
                    else:
                        columns_str.append(sel_field)

                    self.queried_fields.append((column, field))

        elif q.default_cols:
            for field, model in opts.get_fields_with_model():
                if model is None:
                    model = self.query.model
                try:
                    alias = self.query.table_map[model._meta.db_table][0]
                except KeyError:
                    alias = model._meta.db_table

                if field.primary_key:
                    columns_str.append(self._select_id(alias, field.column))

                elif keys_only:
                    continue
                else:
                    columns_str.append(_quote_fields(alias, field.column))
                self.queried_fields.append((field.column, field))

        for alias, aggregate in q.aggregate_select.items():
            col = aggregate.col
            if isinstance(col, tuple):
                colspec = _quote_fields(*col)
            else:
                colspec = col

            if not alias:
                alias = self._gen_alias()

            fn = aggregate.sql_function
            agstr = '{0}({1}) AS {2}'.format(fn, colspec, n1ql_escape(alias))
            self.queried_fields.append((alias, None))
            columns_str.append(agstr)

        return ','.join(columns_str)

    def _require_table(self, alias, name):
        s = []

        if name in self._cbtp_tables:
            return

        self._cbtp_tables.add(name)

        s.append(_quote_fields(alias, TYPEFIELD))
        s.append('=')
        s.append(self._params.indexstr())
        self._params.add(name)
        s = ' '.join(s)
        s = '({})'.format(s)
        self._where.append(s)

    def get_from(self):
        result = []
        first = True
        if not self.query.tables:
            pprint(vars(self.query))
            raise Exception('No tables specified!')

        for alias in self.query.tables:
            jinfo = self.query.alias_map[alias]
            # pprint(jinfo)
            jstr = []

            if first:
                # Ignore anything else
                real_alias = jinfo.table_name or alias
                jstr.append(n1ql_escape(BUCKET_PLACEHOLDER))
                jstr.append(real_alias)
                self._require_table(alias, jinfo.table_name)
                first = False

            else:
                if not jinfo.join_type:
                    raise Exception('Subsequent tables must have join type!')
                jstr.append(jinfo.join_type)  # "INNER JOIN"
                jstr.append(n1ql_escape(BUCKET_PLACEHOLDER))  # `bucket`
                jstr.append(jinfo.rhs_alias)  # django_table
                self._require_table(jinfo.rhs_alias, jinfo.table_name)
                jstr.append('ON KEYS')
                rhs_col, _ = jinfo.join_cols[0]
                jstr.append(_quote_fields(jinfo.lhs_alias, rhs_col))

            result.extend(jstr)

        return ' '.join(result)

    def dict_to_row(self, obj):
        rv = []
        for alias, field in self.queried_fields:
            if field is None:
                rv.append(obj[alias])
            else:
                rv.append(self.connection.ops.convert_values(obj[alias], field))

        return rv

    def execute(self, bucket):
        s = ' '.join(self._ss).replace(BUCKET_PLACEHOLDER, bucket.bucket)

        print 'QUERY:', s
        print 'PARAMS:', self._params.values

        nq = N1QLQuery(s, *self._params.values)
        nq.consistency = CONSISTENCY_REQUEST

        # Bug here, PYCBC-290, if we return the iterator
        return bucket.n1ql_query(nq)


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

                if value is None and not field.primary_key:
                    if field.has_default():
                        value = field.get_default()
                    if value is None:
                        if not field.null:
                            raise Exception('None found, but field is non-nulable')
                        else:
                            continue

                if field.get_internal_type() == 'ForeignKey':
                    value = DocID.encode(table, value)

                # Only for string DocIDs
                if field.primary_key:
                    if value:
                        docid = DocID.encode(table, value)

                if not field.primary_key:
                    value = self.connection.ops.value_for_db(value, field)
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
        return bucket.insert_multi(to_insert)


class UpdateCommand(object):
    def __init__(self, connection, query):
        self.query = query
        self.select = SelectCommand(connection, query, keys_only=True)
        self.connection = connection

    def execute(self, bucket):
        rows = [x for x in self.select.execute(bucket)]
        # pprint(rows)

        ids = [x['id'] for x in rows]
        if not ids:
            return 0

        docs = bucket.get_multi(ids)
        # pprint(vars(self.query))
        to_update = {}

        for res in docs.values():
            merge = {}
            for field, model, value in self.query.values:
                # Get the actual destination name
                if field.get_internal_type() == 'ForeignKey' and \
                                field.related_field.get_internal_type() == 'AutoField':
                    value = DocID.encode(self.query.model._meta.db_table)
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
        ids = [x['id'] for x in rows]
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
        for row in bucket.n1ql_query(nq):
            bucket.remove(row['id'])

        # else:
        #     bucket.flush()


class SQLCompiler(compiler.SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False):
        self.pre_sql_setup()
        self.refcounts_before = self.query.alias_refcount.copy()
        return SelectCommand(self.connection, self.query), None


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def __init__(self, *args, **kwargs):
        self.return_id = None
        super(SQLInsertCompiler, self).__init__(*args, **kwargs)

    def as_sql(self):
        self.pre_sql_setup()
        # pprint(vars(self.query))

        # Always pass down all the fields on an insert
        cmd = InsertCommand(self.connection, self.query.model)
        params = cmd.get_params(self.query.objs, self.query.fields)
        return [(cmd, params)]


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    def __init__(self, *args, **kwargs):
        super(SQLUpdateCompiler, self).__init__(*args, **kwargs)

    def as_sql(self):
        self.pre_sql_setup()
        return UpdateCommand(self.connection, self.query), []


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    def as_sql(self):
        return DeleteCommand(self.connection, self.query), []


class SQLDateTimeCompiler(DateTimeCompiler, SQLCompiler):
    pass


