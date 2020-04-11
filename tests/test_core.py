from avro.schema import SchemaFromJSONData, parse
from io import StringIO
import json
import pytest
from unittest import mock

from avro_models import core
from avro_models import models


TEST_SCHEMA = {
    "name": "Date",
    "namespace": "test",
    "type": "record",
    "fields": [
        {
            "name": "year",
            "type": "int"
        }
    ]
}
TEST_SCHEMA_STRING = json.dumps("int")
TEST_FILE_PATH = "/tmp/test/file"
TEST_SCHEMA_FILE_CONTENT = json.dumps(TEST_SCHEMA)
TEST_CLASS_NAME = "TestClass"
TEST_OUTPUT_CLASS = type(TEST_CLASS_NAME, (models.AvroComplexModel,), {})
TEST_AVRO_SCHEMA = SchemaFromJSONData(TEST_SCHEMA)


class WrapperClass(object):
    pass


@pytest.mark.parametrize("schema_json, schema_file, expected", [
    (None, TEST_FILE_PATH, TEST_SCHEMA_FILE_CONTENT),
    (None, StringIO(TEST_SCHEMA_FILE_CONTENT), TEST_SCHEMA_FILE_CONTENT),
    (json.loads(TEST_SCHEMA_FILE_CONTENT), None, TEST_SCHEMA_FILE_CONTENT),
    (TEST_SCHEMA_STRING, None, TEST_SCHEMA_STRING),
    (TEST_SCHEMA_STRING, TEST_FILE_PATH, TEST_SCHEMA_STRING),
])
def test_import_schema(schema_json, schema_file, expected):
    container = core.AvroModelContainer()
    with mock.patch("avro_models.core.open", return_value=StringIO(TEST_SCHEMA_FILE_CONTENT)):
        assert json.dumps(
            core.import_schema(container, schema_json=schema_json, schema_file=schema_file)
        ) == expected


def test_import_schema_exception():
    with pytest.raises(core.AvroException):
        core.import_schema(None, None)


@mock.patch("avro.schema.SchemaFromJSONData")
@mock.patch("avro.schema.Schema")
@mock.patch("avro_models.core.AvroModelContainer")
@mock.patch("avro_models.core.open")
def test_avro_schema_decorator(mock_open, mock_container, mock_schema, mock_schema_from_json_data):
    mock_open.side_effect = [StringIO(TEST_SCHEMA_FILE_CONTENT), StringIO(TEST_SCHEMA_FILE_CONTENT)]
    mock_schema_from_json_data.return_value = mock_schema
    mock_container.return_value.register_model.return_value = TEST_OUTPUT_CLASS
    mock_container.return_value.GetSchema.side_effect = [
        None, TEST_AVRO_SCHEMA, TEST_AVRO_SCHEMA, TEST_AVRO_SCHEMA]
    container = core.AvroModelContainer()
    assert core.avro_schema(
        container, schema_file=TEST_FILE_PATH
    )(models.AvroComplexModel).__name__ == TEST_CLASS_NAME
    assert mock_schema_from_json_data.call_count, 1
    core.avro_schema(
        container, schema_file=TEST_FILE_PATH
    )(models.AvroComplexModel)
    assert mock_schema_from_json_data.call_count, 1
    core.avro_schema(
        container, full_name="test.Date"
    )(models.AvroComplexModel)
    assert mock_schema_from_json_data.call_count, 1


@mock.patch("avro.schema.SchemaFromJSONData")
@mock.patch("avro.schema.Schema")
@mock.patch("avro_models.core.create_data_model", return_value=TEST_OUTPUT_CLASS)
def test_register_model(mock_model, mock_schema, mock_schema_from_json_data):
    mock_schema_from_json_data.return_value = mock_schema
    container = core.AvroModelContainer()
    schema = SchemaFromJSONData(TEST_SCHEMA, container)
    assert container.register_model(schema, WrapperClass) == TEST_OUTPUT_CLASS
    assert len(container._schema_map) == 1
