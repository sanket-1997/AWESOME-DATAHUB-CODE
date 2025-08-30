from libs import schema_handler as sh

field_schema , primary_keys, table_type, dependencies = sh.read_schema(env = 'dev', layer='gold', source='consumer', entity='dim_customers')


print(field_schema)
print(primary_keys)
print(table_type)
print(dependencies)

