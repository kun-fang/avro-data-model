from avro.schema import SchemaFromJSONData
import pytest

from avro_models import core
from avro_models import models


TEST_RECORD_SCHEMA = {
    "name": "TestRecord",
    "type": "record",
    "fields": [
        {
            "name": "field1",
            "type": "int"
        },
        {
            "name": "field2",
            "type": "string"
        }
    ]
}

TEST_ENUM_SCHEMA = {
    "name": "TestEnum",
    "type": "enum",
    "symbols": ["SYMBOL1", "SYMBOL2", "SYMBOL3"]
}

TEST_ARRAY_SCHEMA = {
    "name": "TestArray",
    "type": "array",
    "items": "string"
}

TEST_MAP_SCHEMA = {
    "name": "TestMap",
    "type": "map",
    "values": "long"
}

TEST_FIXED_SCHEMA = {
    "name": "TestFixed",
    "type": "fixed",
    "size": 16,
}

TEST_UNION_SCHEMA = ["null", "string"]


class WrapperClass(object):
    pass


@pytest.mark.parametrize("schema_json, expected_type", [
    (TEST_RECORD_SCHEMA, models.RecordAvroModel),
    (TEST_ENUM_SCHEMA, models.EnumAvroModel),
    (TEST_ARRAY_SCHEMA, models.ArrayAvroModel),
    (TEST_MAP_SCHEMA, models.MapAvroModel),
    (TEST_FIXED_SCHEMA, models.FixedAvroModel),
    (TEST_UNION_SCHEMA, models.UnionAvroModel),
])
def test_create_data_model_successful(schema_json, expected_type):
    schema = SchemaFromJSONData(schema_json)
    data_cls = models.create_data_model(
        schema, WrapperClass, core.AvroModelContainer())
    assert issubclass(data_cls, expected_type)
    assert data_cls.get_schema() == schema


def test_record_schema_model():
    schema = SchemaFromJSONData(TEST_RECORD_SCHEMA)
    record_cls = models.create_data_model(
        schema, WrapperClass, core.AvroModelContainer())
    assert record_cls.get_fields() == schema.fields


def test_enum_schema_model():
    schema = SchemaFromJSONData(TEST_ENUM_SCHEMA)
    data_cls = models.create_data_model(
        schema, WrapperClass, core.AvroModelContainer())
    assert set(data_cls.get_symbols()) == set(TEST_ENUM_SCHEMA['symbols'])


def test_array_schema():
    names = core.AvroModelContainer()
    schema = SchemaFromJSONData(TEST_ARRAY_SCHEMA)
    data_cls = models.create_data_model(schema, WrapperClass, names)
    assert data_cls.get_contained_class() == str


def test_map_schema():
    names = core.AvroModelContainer()
    schema = SchemaFromJSONData(TEST_MAP_SCHEMA)
    data_cls = models.create_data_model(schema, WrapperClass, names)
    assert data_cls.get_contained_class() == int


def test_fixed_schema():
    names = core.AvroModelContainer()
    schema = SchemaFromJSONData(TEST_FIXED_SCHEMA)
    data_cls = models.create_data_model(schema, WrapperClass, names)
    assert data_cls.get_size() == TEST_FIXED_SCHEMA['size']
