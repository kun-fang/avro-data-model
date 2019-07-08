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
def test_import_schema(schema_json, schema_file, expected):
    with mock.patch("avro_models.core.open", return_value=StringIO(TEST_SCHEMA_FILE_CONTENT)):
        assert json.dumps(core.import_schema(schema_json, schema_file)) == expected


def test_import_schema_exception():
    with pytest.raises(core.AvroException):
        core.import_schema(None, None)


@mock.patch("avro.schema.SchemaFromJSONData")
@mock.patch("avro.schema.Schema")
@mock.patch("avro_models.core.AvroModelContainer")
@mock.patch("avro_models.core.open", return_value=StringIO(TEST_SCHEMA_FILE_CONTENT))
def test_avro_schema_decorator(mock_open, mock_container, mock_schema, mock_schema_from_json_data):
    mock_schema_from_json_data.return_value = mock_schema;
    mock_container.return_value.register_model.return_value = TEST_OUTPUT_CLASS
    assert core.avro_schema(
        core.AvroModelContainer(),
        schema_file=TEST_FILE_PATH
    )(models.AvroComplexModel).__name__ == TEST_CLASS_NAME


@mock.patch("avro.schema.SchemaFromJSONData")
@mock.patch("avro.schema.Schema")
@mock.patch("avro_models.core.create_data_model", return_value=TEST_OUTPUT_CLASS)
def test_register_model(mock_model, mock_schema, mock_schema_from_json_data):
    mock_schema_from_json_data.return_value = mock_schema
    container = core.AvroModelContainer()
    schema = SchemaFromJSONData(TEST_SCHEMA, container)
    assert container.register_model(schema, WrapperClass) == TEST_OUTPUT_CLASS
    assert len(container._schema_map) == 1
