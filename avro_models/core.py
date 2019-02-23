from avro.schema import Names, SchemaFromJSONData, AvroException
import json
from io import IOBase
from six import string_types

from .models import create_data_model


class AvroModelContainer(Names):
    def __init__(self, default_namespace=None):
        super(AvroModelContainer, self).__init__(
            default_namespace=default_namespace)
        self._schema_map = {}

    def register_model(self, schema, wrapper_cls):
        model_cls = create_data_model(schema, wrapper_cls, self)
        self._schema_map[schema.fullname] = model_cls
        return model_cls

    def get_avro_model(self, schema_full_name):
        return self._schema_map[schema_full_name]

    def has_schema(self, schema_full_name):
        return schema_full_name in self._schema_map

    def __repr__(self):
        return str(self._schema_map)


def import_schema(schema_json=None, schema_file=None):
    if schema_json:
        if isinstance(schema_json, string_types):
            return json.loads(schema_json)
    elif schema_file:
        if isinstance(schema_file, string_types):
            with open(schema_file, "rb") as f:
                return json.load(f)
        if isinstance(schema_file, IOBase):
            return json.loads(schema_file.read())
        if isinstance(schema_json, dict):
            return schema_json
    raise AvroException("invalid input schema")


def avro_schema(container, **kwargs):
    schema_json = import_schema(**kwargs)

    def wrapper(cls):
        schema = SchemaFromJSONData(schema_json, container)
        return container.register_model(schema, cls)
    return wrapper
