from libs import utils
import yaml





#this will return field schema so we will get most of the value
def read_schema(env = None, layer = None, source = None, entity = None ) -> dict:
    data = utils.open_yaml_file(utils.generate_dc_file_path(env,layer,source,entity))
    field_schema = data["models"][f"datahub_{env}_{layer}-{source}-{entity}"]["fields"]
    return field_schema

#this will return set of attributes and will be used for schema evolution
def extract_attributes(field_schema) -> list:
    return list(set(field_schema))


#this is to decide whether to evolve the schema or not
def schema_evolution_decider(source_attributes: list, 
                         target_attributes: list, 
                         contract_attributes: list) -> bool:

    source_set = set(source_attributes)
    target_set = set(target_attributes)
    contract_set = set(contract_attributes)

    # Find new attributes in source table that are not in target yet
    new_raw_attributes = source_set - target_set

    # If all new attributes are allowed by contract → True
    if new_raw_attributes.issubset(contract_set):
        return True
    else:
        return False


a= read_schema('dev', 'silver', 'consumer', 'dim_customers')

print(yaml.dump(a,sort_keys=False, indent=2))
print(extract_attributes(a))



