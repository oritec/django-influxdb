from django.db.models.lookups import Range
from django.db.models.fields import Field
from django.db.models import Lookup
from django.db.models.expressions import Col

class InfluxDB(Range):
    #def as_sql(self,compiler, connection):
    #    return None
    def as_influxdata(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = rhs_params
        return '%s >= %s AND %s <= %s' % (lhs, rhs[0],lhs,rhs[1]), params

class NotEqual(Lookup):
    lookup_name = 'ne'

    def as_influxdata(self, qn, connection):
        lhs, lhs_params = self.process_lhs(qn, connection)
        rhs, rhs_params = self.process_rhs(qn, connection)
        params = lhs_params + rhs_params
        return '%s != %s' % (lhs, rhs), params

Field.register_lookup(InfluxDB)
Field.register_lookup(NotEqual)

def col_as_influxdb(self, compiler, connection):
    qn = compiler.quote_name_unless_alias
    return "%s" % (qn(self.target.column)), []

Col.as_influxdata = col_as_influxdb