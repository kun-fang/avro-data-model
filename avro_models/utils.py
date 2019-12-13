import json


def is_equal(schema1, schema2):
    schema1 = json.dumps(schema1, sort_keys=True)
    schema2 = json.dumps(schema2, sort_keys=True)
    return schema1 == schema2


def get_full_name(schema):
    namespace = schema.get("namespace", "")
    name = schema["name"]
    return name if '.' in name else '%s.%s' % (namespace, name)

