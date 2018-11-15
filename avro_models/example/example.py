import datetime
import os

from avro_models.core import avro_schema, AvroDataNames


EXAMPLE_NAMES = AvroDataNames(default_namespace="example.avro")
DIRNAME = os.path.dirname(os.path.realpath(__file__))


@avro_schema(EXAMPLE_NAMES, schema_file=os.path.join(DIRNAME, "Date.avsc"))
class Date(object):
  def __init__(self, value):
    print(value)
    print(self.__class__)
    if isinstance(value, datetime.date):
      value = {
          'year': value.year,
          'month': value.month,
          'day': value.day
      }
    super().__init__(value)

  @property
  def date(self):
    return datetime.date(self.year, self.month, self.day)

  def validate(self, data):
    return super().validate(data) \
        and datetime.date(data['year'], data['month'], data['day'])

  def __str__(self):
    return str(self.date)


@avro_schema(EXAMPLE_NAMES, schema_file=os.path.join(DIRNAME, "Occupation.avsc"))
class Occupation(object):
  pass


@avro_schema(EXAMPLE_NAMES, schema_file=os.path.join(DIRNAME, "User.avsc"))
class User(object):

  @property
  def fullname(self):
    return "{} {}".format(self.firstName, self.lastName)

  def __repr__(self):
    return "User({})".format(self.fullname)


if __name__ == '__main__':
  print(EXAMPLE_NAMES)
  item = {"year": 1985, "month": 4, "day": 3}
  date = Date(item)

  item = datetime.datetime(2017, 7, 20)
  date = Date(item)
  print(date, type(date))

  data = {
      'favorite_color': None,
      'favorite_number': 256,
      'firstName': 'Alyssa',
      'lastName': 'Yssa',
      'occupation': 'STUDENT',
      'birthday': {
          'year': 1985,
          'month': 4,
          'day': 3
      }
  }
  user = User(data)
  print(repr(user))
  print(user.fullname)
  print(user.json_dumps())

  print(type(date))
  user.birthday = date
  print(user)