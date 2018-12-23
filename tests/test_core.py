from avro.schema import Schema, SchemaFromJSONData
from io import StringIO
import json
import pytest
from unittest import mock

from avro_models import core
from avro_models import models

TEST_SCHEMA_STRING = json.dumps("int")
TEST_FILE_PATH = "/tmp/test/file"
TEST_SCHEMA_FILE_CONTENT = json.dumps("long")
TEST_CLASS_NAME = "TestClass"
TEST_OUTPUT_CLASS = type(TEST_CLASS_NAME, (models.AvroComplexModel,), {})

TEST_SCHEMA = {
    "name": "Date",
    "type": "record",
    "fields": [
        {
            "name": "year",
            "type": "int"
        }
    ]
}


class WrapperClass(object):
  pass


@pytest.mark.parametrize("schema_json, schema_file, expected", [
    (None, TEST_FILE_PATH, TEST_SCHEMA_FILE_CONTENT),
    (None, StringIO(TEST_SCHEMA_FILE_CONTENT), TEST_SCHEMA_FILE_CONTENT),
    (None, json.loads(TEST_SCHEMA_FILE_CONTENT), TEST_SCHEMA_FILE_CONTENT),
    (TEST_SCHEMA_STRING, None, TEST_SCHEMA_STRING),
    (TEST_SCHEMA_STRING, TEST_FILE_PATH, TEST_SCHEMA_STRING),
])
def test_import_schema(monkeypatch, schema_json, schema_file, expected):
  monkeypatch.setattr("builtins.open", lambda x, y: StringIO(TEST_SCHEMA_FILE_CONTENT))
  assert json.dumps(core.import_schema(schema_json, schema_file)) == expected


@pytest.mark.xfail(raises=core.AvroException)
def test_import_schema_exception():
  core.import_schema(None, None)


def test_avro_schema_decorator(monkeypatch):
  container = core.AvroModelContainer()
  schema = mock.MagicMock(spec=Schema)
  monkeypatch.setattr("builtins.open", lambda x, y: StringIO(TEST_SCHEMA_FILE_CONTENT))
  monkeypatch.setattr("avro_models.core.SchemaFromJSONData", lambda x, y: schema)
  monkeypatch.setattr(container, "register_model", lambda x, y: TEST_OUTPUT_CLASS)
  assert core.avro_schema(container, schema_file=TEST_FILE_PATH)(models.AvroComplexModel) \
      .__name__ == TEST_CLASS_NAME


def test_register_model(monkeypatch):
  container = core.AvroModelContainer()
  schema = SchemaFromJSONData(TEST_SCHEMA, container)
  monkeypatch.setattr("avro_models.core.create_data_model", lambda x, y, z: TEST_OUTPUT_CLASS)
  assert container.register_model(schema, WrapperClass) == TEST_OUTPUT_CLASS
  assert len(container._schema_map) == 1
