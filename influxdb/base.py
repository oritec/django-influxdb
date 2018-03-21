"""
Dummy database backend for Django.

Django uses this if the database ENGINE setting is empty (None or empty string).

Each of these API functions, except connection.close(), raise
ImproperlyConfigured.
"""

from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.dummy.features import DummyDatabaseFeatures

from influxdb import InfluxDBClient

from backends.influddbAPI2 import dbapi2 as Database
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime, parse_time
import datetime
from django.conf import settings

def complain(*args, **kwargs):
    raise ImproperlyConfigured("settings.DATABASES is improperly configured. "
                               "Please supply the ENGINE value. Check "
                               "settings documentation for more details.")

def ignore(*args, **kwargs):
    pass

class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        if name.startswith("`") and name.endswith("`"):
            return name  # Quoting once is enough.
        return name

    def adapt_datetimefield_value(self, value):
        if value is None:
            return None

        # Expression values are adapted by the database.
        if hasattr(value, 'resolve_expression'):
            return value

        # MySQL doesn't support tz-aware datetimes
        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = timezone.make_naive(value, self.connection.timezone)
            else:
                raise ValueError("MySQL backend does not support timezone-aware datetimes when USE_TZ is False.")

        if not self.connection.features.supports_microsecond_precision:
            value = value.replace(microsecond=0)

        return str(value)

    # Django busca acá como convertir los diferentes tipos de datos que pide la query. TODO: Implementar el resto.
    def get_db_converters(self, expression):
        converters = super().get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type == 'DateTimeField':
            converters.append(self.convert_datetimefield_value)
        # elif internal_type == 'DateField':
        #     converters.append(self.convert_datefield_value)
        # elif internal_type == 'TimeField':
        #     converters.append(self.convert_timefield_value)
        # elif internal_type == 'DecimalField':
        #     converters.append(self.convert_decimalfield_value)
        # elif internal_type == 'UUIDField':
        #     converters.append(self.convert_uuidfield_value)
        # elif internal_type in ('NullBooleanField', 'BooleanField'):
        #     converters.append(self.convert_booleanfield_value)
        return converters

    def convert_datetimefield_value(self, value, expression, connection):
        if value is not None:
            if not isinstance(value, datetime.datetime):
                value = parse_datetime(value)
        if value is not None:
            if settings.USE_TZ and not timezone.is_aware(value):
                value = timezone.make_aware(value, self.connection.timezone)
        return value

class DatabaseClient(BaseDatabaseClient):
    #Resulta ser que influxDB si tiene un CLI, revisar
    def runshell(self):
        return 0


class DatabaseCreation(BaseDatabaseCreation):
    create_test_db = ignore
    destroy_test_db = ignore

class CursorWrapper:
    """
    A thin wrapper around MySQLdb's normal cursor class that catches particular
    exception instances and reraises them with the correct types.

    Implemented as a wrapper, rather than a subclass, so that it isn't stuck
    to the particular underlying representation returned by Connection.cursor().
    """
    codes_for_integrityerror = (
        1048,  # Column cannot be null
        1690,  # BIGINT UNSIGNED value is out of range
    )

    def __init__(self, cursor):
        self.cursor = cursor
        self.count = 0
        self.last_fetch_count = 0
        self.results = []

    def prepare_query(self,query, args= None):
        if args is not None:
            args2 = ()
            # TODO: Preparacion de quoting para fechas tambien y en general mas tipos de datos
            for arg in args:
                if type(arg) is str:
                    args2 += ("'"+arg+"'",)
                else:
                    args2 += (arg,)
            query = query % args2
        return query.replace("datos.", "")

    def check_aggregations(self,query):
        select = 'SELECT '
        clause = 'GROUP BY '
        [select_part, from_part] = query[query.find(select)+len(select):].split('FROM')
        vars = select_part.split(',')
        aggr = []
        for var in query[query.find(clause)+len(clause):].split(','):
            try:
                if not var.strip() == 'time':
                    idx = vars.index(var)
                    self.aggregations.append({var:idx})
                    aggr.append(var)
                    vars.remove(var)
            except:
                pass
        return select + ','.join(vars) + ' FROM' + from_part[:from_part.find(clause)] + clause + ','.join(aggr)

    def execute(self, query, args=None):
        try:
            # args is None means no string interpolation
            if args is None:
                query_prepared = self.prepare_query(query)
            else:
                query_prepared = self.prepare_query(query, args)
            # TODO: Ahora hay que hacer el tema del group, sacar desde el listado de SELECT, y luego integrarlo en el resultado.
            self.aggregations = []
            if query_prepared.find('GROUP BY') > -1 :
                query_prepared = self.check_aggregations(query_prepared)

            res = self.cursor.query(query_prepared)
            # Parece que devuelve cuantas filas son...
            time_on_select = False
            if query_prepared.find('SELECT')>-1:
                if query_prepared[:query_prepared.find('FROM')].find('time') > -1:
                    time_on_select = True

            v = []
            for r in res._get_series():
                if not time_on_select:
                    for aux in r['values']:
                        aux.pop(0)

                if len(self.aggregations) > 0:
                    for ag in self.aggregations:
                        for key in ag:
                            if key.strip() != 'time':
                                val = r['tags'][key]
                                for aux in r['values']:
                                    aux.insert(ag[key],val)
                v = v + r['values']
            self.count = len(v)
            self.results = v
            return self.count
        except Exception as e:
            raise

    def executemany(self, query, args):
        try:
            return self.cursor.executemany(query, args)
        except:
            raise

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

    def fetchmany(self, count):
        #return [{'datos.time':datetime.now(),'datos.turbina':'WTG01', 'datos.Prod_TotAccumulated_ActPwrGen2':1397149}]
        if self.count > 0:
            # TODO: Check con carga de datos masiva
            if self.count <= count:
                inicio = self.last_fetch_count
                final = self.last_fetch_count + self.count
                self.count = 0

            else :
                inicio = self.last_fetch_count
                final = self.last_fetch_count + count
                self.count -= count
                self.last_fetch_count +=  count
            return self.results[inicio:final]
        else:
            return []
    def fetchone(self):
        if self.count > 0:
            self.count -= 1
            idx = self.last_fetch_count
            self.last_fetch_count += 1
            return self.results[idx]
        else:
            return []

    def fetchall(self):
        if self.count > 0:
            return self.results
        else:
            return []

class DatabaseIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        pass

    def get_table_description(self, cursor, table_name):
        pass

    def get_relations(self, cursor, table_name):
        pass

    def get_indexes(self, cursor, table_name):
        pass

    def get_key_columns(self, cursor, table_name):
        pass


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'influxdata'
    display_name = 'InfluxDB'
    # TODO: Lo unico que he probado hasta el momento es exact
    operators = {
        'exact': '= %s',
        'iexact': 'LIKE %s',
        'contains': 'LIKE BINARY %s',
        'icontains': 'LIKE %s',
        'regex': 'REGEXP BINARY %s',
        'iregex': 'REGEXP %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE BINARY %s',
        'endswith': 'LIKE BINARY %s',
        'istartswith': 'LIKE %s',
        'iendswith': 'LIKE %s',
    }
    Database = Database
    usable = False
    # Override the base class implementations with null
    # implementations. Anything that tries to actually
    # do something raises complain; anything that tries
    # to rollback or undo something raises ignore.
    def get_connection_params(self):
        kwargs = { }

        settings_dict = self.settings_dict

        if settings_dict['USER']:
            kwargs['user'] = settings_dict['USER']
        if settings_dict['NAME']:
            kwargs['db'] = settings_dict['NAME']
        if settings_dict['PASSWORD']:
            kwargs['passwd'] = settings_dict['PASSWORD']
        if settings_dict['HOST']:
            kwargs['host'] = settings_dict['HOST']
        if settings_dict['PORT']:
            kwargs['port'] = int(settings_dict['PORT'])
        return kwargs

    def get_new_connection(self, conn_params):
        # Hay que ver que pasa si no se puede realizar conexion
        return InfluxDBClient(conn_params['host'], conn_params['port'], '', '', conn_params['db'])
        #return None

    def init_connection_state(self):
        pass

    def create_cursor(self, name=None):
        return CursorWrapper(self.connection)

    def ensure_connection(self):
        # Hay que verificar la conexion aca, la libreria de influxdb tiene un metodo ping
        if self.connection is None:
            with self.wrap_database_errors:
                self.connect()

    def _set_autocommit(self, autocommit):
        pass
        #with self.wrap_database_errors:
        #    self.connection.autocommit(autocommit)

    _commit = complain
    _rollback = ignore
    _close = ignore
    _savepoint = ignore
    _savepoint_commit = complain
    _savepoint_rollback = ignore
    #_set_autocommit = complain
    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DummyDatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def is_usable(self):
        return self.usable
