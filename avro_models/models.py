

class AvroComplexModel(object):
  __schema__ = None

  def __init__(self, value):
    self._value = value

  def __str__(self):
    return str(self._value)

  def __repr__(self):
    return "{}({})".format(self.__name__, self._value)

  def __eq__(self, other):
    return self._value == other._value

  @classmethod
  def get_schema(cls):
    return cls.__schema__


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


class ContainerAvroModel(AvroComplexModel):
  def __init__(self, value):
    item_class = self.get_container_class()
    self._value = self._new_instance(value, item_class)

  @staticmethod
  def _get_container_schema(schema):
    raise NotImplementedError

  @classmethod
  def _new_instance(cls, value, item_class):
    raise NotImplementedError

  @classmethod
  def get_container_class(cls):
    return get_avro_model(cls._get_container_schema(cls.__schema__))


class ArrayAvroModel(ContainerAvroModel):

  @staticmethod
  def _get_container_schema(schema):
    return schema.items

  @classmethod
  def _new_instance(cls, value, item_class):
    return [item_class(x) for x in value]

  def __getattr__(self, attr):
    return getattr(self._value, attr)

  def __len__(self):
    return len(self._value)

  def __iter__(self):
    return iter(self._value)

  def __getitem__(self, key):
    return self._value[key]

  def __setitem__(self, key, value):
    self._value[key] = self.get_container_class()(value)


class MapAvroModel(ContainerAvroModel):

  @staticmethod
  def _get_container_schema(schema):
    return schema.values

  @classmethod
  def _new_instance(cls, value, item_class):
    return {k: item_class(v) for k, v in value.items()}

  @classmethod
  def get_container_class(cls):
    return get_avro_model(cls._get_container_schema(cls.__schema__))

  def __len__(self):
    return len(self._value)

  def __iter__(self):
    return iter(self._value)

  def __getitem__(self, key):
    return self._value[key]

  def __setitem__(self, key, value):
    self._value[key] = self.get_container_class()(value)


class UnionAvroModel(ContainerAvroModel):
  @staticmethod
  def _get_container_schema(schema):
    return schema.schemas

  @classmethod
  def get_container_class(cls):
    return [get_avro_model(schema) for schema in cls._get_container_schema(cls.__schema__)]

  @classmethod
  def _new_instance(cls, value, union_classes):
    for item_class in union_classes:
      try:
        return item_class(value)
      except:
        pass
    raise ValueError("No matching data type is found. Expecting {}".format(
        cls.__schema__.to_json()))


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
    'union': UnionAvroModel
}


def get_avro_model(schema):
  """
  Generate Avro data model for a schema

  Args:
    schema: input schema as avrol.schema.Schema

  Returns:
    Avro data model constructor
  """
  schema_type = schema.type
  if schema_type in PRIMITIVE_SCHEMA_MAP:
    return PRIMITIVE_SCHEMA_MAP[schema_type]
  if schema_type in NAMED_SCHEMA_MAP:
    return get_named_schema_model(schema)
  if schema_type in OTHER_SCHEMA_MAP:
    return get_other_schema_model(schema)


def get_named_schema_model(schema):
  """
  Get extended model class for the named schema

  Args:
    schema: input schema as avro.schema.Schema

  Returns:
    Extended Avro named schema data model
  """
  class VirtualClass(NAMED_SCHEMA_MAP[schema.type]):
    __schema__ = schema
    __name__ = schema.name
    __qualname__ = schema.name

  return VirtualClass


def get_other_schema_model(schema):
  base_class = OTHER_SCHEMA_MAP[schema.type]
  class_name = "{}<{}>".format(schema.type, base_class._get_container_schema(schema).fullname)

  class VirtualClass(base_class):
    __schema__ = schema
    __name__ = class_name
    __qualname__ = class_name

  return VirtualClass
