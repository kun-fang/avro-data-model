# pylint: disable=no-member
import json

from avro.io import Validate, AvroTypeException
from avro.schema import AvroException


class AvroComplexModel(object):
    __schema__ = None
    _names = None

    def __init__(self, value):
        dict_value = _recursive_to_dict(value)
        if not self.validate(dict_value):
            raise AvroTypeException(self.__schema__, value)
        super().__setattr__("_value", dict_value)

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return "{}({})".format(self.__name__, self._value)

    def __eq__(self, other):
        return self._value == other._value

    def validate(self, data):
        return Validate(self.__schema__, data)

    @classmethod
    def get_schema(cls):
        return cls.__schema__

    def to_dict(self):
        return self._value

    def json_dumps(self):
        return json.dumps(self._value)


class EnumAvroModel(AvroComplexModel):
    @classmethod
    def get_symbols(cls):
        return cls.__schema__.symbols


class FixedAvroModel(AvroComplexModel):
    @classmethod
    def get_size(cls):
        return cls.__schema__.size


class RecordAvroModel(AvroComplexModel):
    @classmethod
    def get_fields(cls):
        return cls.__schema__.fields

    def __getattr__(self, attr):
        return self._value[attr]

    def __setattr__(self, attr, value):
        field = self.__schema__.field_map[attr]
        item_class = find_avro_model(field.type, self._names)
        item = item_class(value)
        self._value[attr] = item._value


class ContainerAvroModel(AvroComplexModel):

    @classmethod
    def _get_contained_schema(cls, schema):
        raise NotImplementedError

    @classmethod
    def get_contained_class(cls):
        return find_avro_model(
            cls._get_contained_schema(cls.__schema__),
            cls._names)

    def __len__(self):
        return len(self._value)

    def __iter__(self):
        return iter(self._value)

    def __getitem__(self, key):
        item_class = self.get_contained_class()
        return item_class(self._value[key])

    def __setitem__(self, key, value):
        item_class = self.get_contained_class()
        item = item_class(self._value[key])
        self._value[key] = item._value


class ArrayAvroModel(ContainerAvroModel):

    @staticmethod
    def _get_contained_schema(schema):
        return schema.items


class MapAvroModel(ContainerAvroModel):

    @staticmethod
    def _get_contained_schema(schema):
        return schema.values


class UnionAvroModel(AvroComplexModel):
    __name__ = "Union"
    __qualname__ = "Union"


def get_null(value):
    if value is None:
        return None
    else:
        raise ValueError('"null" must be None')


PRIMITIVE_SCHEMA_MAP = {
    "null": get_null,
    "boolean": bool,
    "int": int,
    "long": int,
    "float": float,
    "double": float,
    "bytes": lambda x: bytes(x, 'utf-8'),
    "string": str
}


NAMED_SCHEMA_MAP = {
    "enum": EnumAvroModel,
    "fixed": FixedAvroModel,
    "record": RecordAvroModel
}

OTHER_SCHEMA_MAP = {
    "array": ArrayAvroModel,
    "map": MapAvroModel,
    "union": UnionAvroModel
}


def get_union_schema_model(wrapper_cls, schema, names):
    return type(wrapper_cls.__qualname__, (wrapper_cls, UnionAvroModel), {
        "_names": names,
        "__schema__": schema
    })


def get_named_schema_model(wrapper_cls, schema, names):
    """
    Get extended model class for the named schema

    Args:
      schema: input schema as avro.schema.Schema

    Returns:
      Extended Avro named schema data model
    """
    return type(
        wrapper_cls.__qualname__,
        (wrapper_cls, NAMED_SCHEMA_MAP[schema.type]),
        {
            "_names": names,
            "__schema__": schema,
            "__qualname__": schema.name,
            "__name__": schema.name
        })


def get_other_schema_model(wrapper_cls, schema, names):
    base_class = OTHER_SCHEMA_MAP[schema.type]
    class_name = "{}<{}>".format(
        schema.type, base_class._get_contained_schema(schema).fullname)

    return type(wrapper_cls.__qualname__, (wrapper_cls, base_class), {
        "_names": names,
        "__schema__": schema,
        "__qualname__": class_name,
        "__name__": class_name
    })


def create_data_model(schema, wrapper_cls, names):
    schema_type = schema.type
    if schema_type in PRIMITIVE_SCHEMA_MAP:
        return PRIMITIVE_SCHEMA_MAP[schema_type]
    if schema_type == 'union':
        return get_union_schema_model(wrapper_cls, schema, names)
    if schema_type in NAMED_SCHEMA_MAP:
        return get_named_schema_model(wrapper_cls, schema, names)
    if schema_type in OTHER_SCHEMA_MAP:
        return get_other_schema_model(wrapper_cls, schema, names)
    raise AvroException("Unsupported Schema: " + schema)


def find_avro_model(schema, names):
    """
    Generate Avro data model for a schema

    Args:
      schema: input schema as avro.schema.Schema

    Returns:
      Avro data model constructor
    """
    if schema.type in PRIMITIVE_SCHEMA_MAP:
        return PRIMITIVE_SCHEMA_MAP[schema.type]
    elif names.has_schema(schema.fullname):
        return names.get_avro_model(schema.fullname)
    else:
        return None


def _recursive_to_dict(data):
  if isinstance(data, dict):
    return {key: _recursive_to_dict(data[key]) for key in data.keys()}
  elif isinstance(data, list):
    return [_recursive_to_dict(x) for x in data]
  elif isinstance(data, AvroComplexModel):
    return data.to_dict()
  else:
    return data

