from libs import schema_handler as sh
from libs import utils as ut
from libs import datafunctions as spdf
field_schema , primary_keys, table_type, dependencies = sh.read_schema(env = 'dev', layer='gold', source='consumer', entity='dim_customers')


print(field_schema)
print(primary_keys)
print(table_type)
print(dependencies)
print(ut.str_to_date('9999-12-31'))

