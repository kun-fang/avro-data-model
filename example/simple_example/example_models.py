import datetime
import os

from avro_models import avro_schema, AvroModelContainer


EXAMPLE_NAMES = AvroModelContainer(default_namespace="example.avro")
DIRNAME = os.path.dirname(os.path.realpath(__file__))


@avro_schema(
    EXAMPLE_NAMES,
    schema_file=os.path.join(DIRNAME, "Date.avsc"))
class Date(object):
    def __init__(self, value):
        if isinstance(value, datetime.date):
            value = {
                'year': value.year,
                'month': value.month,
                'day': value.day
            }
        super().__init__(value)

    def date(self):
        return datetime.date(self.year, self.month, self.day)

    def validate(self, data):
        print("validate", data)
        return super().validate(data) \
            and datetime.date(data['year'], data['month'], data['day'])

    def __str__(self):
        return str(self.date())


@avro_schema(
    EXAMPLE_NAMES,
    schema_file=os.path.join(DIRNAME, "Occupation.avsc"))
class Occupation(object):
    pass


@avro_schema(
    EXAMPLE_NAMES,
    schema_file=os.path.join(DIRNAME, "User.avsc"))
class User(object):
    def fullname(self):
        return "{} {}".format(self.firstName, self.lastName)

    def __repr__(self):
        return "User({})".format(self.fullname())
