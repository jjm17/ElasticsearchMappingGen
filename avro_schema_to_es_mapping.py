''' Produce an Elasticsearch mapping for an AVRO Schema'''
import json
import avro.schema

# Map of Avro primitive types to Elasticsearch types
avro_primitive_to_es_type = {
    "int": "integer",
    "string": "keyword",
    "bytes": "binary",
    "boolean": "boolean",
    "long": "long",
    "float": "float",
    "double": "double"
}

# Elasticsearch field options
es_field_type_options = {
    "integer": {},
    # Ignore long strings.
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html
    # Assume no need for scoring.
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/norms.html
    # Doc number and term frequencies are indexed.
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/index-options.html
    "keyword": {"ignore_above": 256, "norms": False, "index_options": "freqs"},
    "binary": {},
    "boolean": {},
    "long": {},
    "float": {},
    "double": {}
}

avro_complex = ["record", "union", "array", "enum", "map", "fixed"]


def from_avro_primitive_type_to_es_mapping(schema):
    '''Get the Elasticsearch type for an AVRO Primtive type'''
    if schema.type in avro_primitive_to_es_type:
        es_type = avro_primitive_to_es_type[schema.type]
        if es_type == "keyword":
            es_field_options = {}
            es_field_options["type"] = es_type
            es_field_options.update(es_field_type_options[es_type])
            return es_field_options
        else:
            return {"type": es_type}
    return None


def from_avro_complex_type_to_es_mapping(schema):
    '''Elasticsearch mapping for an AVRO Complex type'''
    mapping = {}
    for field in schema.fields:
        if field.type.type == 'record':
            mapping[field.name] = {"properties": to_es_mapping(field.type)}
        elif field.type.type == 'union':
            # For unions, map the non-null fields
            # Possible approach for different types in the same field?
            # https://stackoverflow.com/questions/26930587/elasticsearch-mapping-different-data-types-in-same-field
            sub_mapping = {}
            for sub_schema in filter(
                    lambda s: s.type != "null", field.type.schemas):
                sub_mapping = to_es_mapping(sub_schema)
            mapping[field.props['name']] = sub_mapping
        elif field.type.type == 'array':
            mapping[field.name] = to_es_mapping(field.type.items)
        elif field.type.type in avro_primitive_to_es_type:
            mapping[field.name] = to_es_mapping(field.type)
        elif field.type.type in avro_complex:
            print(field.type.type + " is not supported")
    return mapping


def to_es_mapping(schema):
    ''' Produce an Elasticsearch mapping for an AVRO Schema'''
    if schema.type in avro_primitive_to_es_type:
        return from_avro_primitive_type_to_es_mapping(schema)
    if schema.type in avro_complex:
        return from_avro_complex_type_to_es_mapping(schema)
    return None


def from_avro_schema_to_es_mapping(filename):
    ''' Produce an Elasticsearch mapping for the given AVRO Schema file'''
    avro_schema = avro.schema.parse(open(filename, "rb").read())
    # The Avro schema is assumed to precisely define subsequent data
    # - no unexpected fields!
    es_mapping = {"mappings": {"dynamic": "strict"}}
    es_mapping['mappings']["properties"] = to_es_mapping(avro_schema)
    return json.dumps(es_mapping)
