Avro Data Model
=====

## Introduction
[Apache Avro](http://avro.apache.org/) is a data serialization framework. It has been widely used in data serialization (especially in hadoop ecosystem) and RPC protocols. It has libraries to support many languages. The library supports code generation with static languages like Java, while for dynamic languages for example python, code generation is not necessary.

Avro schemas are also widely used in big data and streaming systems. When avro data is deserialized in Python environment, it was stored as a dict in memory. It looses all the interesting features provided by the avro schema, for example, you can modify an integer field with a string without getting any errors. It also doesn't provide any nice features from a normal class, for example, if an avro schema has `firstName` and `lastName` fields, to provide the `fullName`, it is not easy to define a function call to provide that.

**The project is still under development. Bugs are expected.**

## Example
### A Simple Example
**User.avsc**
```
{
  "type": "record",
  "name": "User",
  "fields": [
    {
      "name": "lastName",
      "type": "string"
    },
    {
      "name": "firstName",
      "type": "string"
    }
  ]
}
```
The following code defined a User class associated with the schema
```
@avro_schema(AvroDataNames(default_namespace="example.avro"), schema_file="User.avsc")
class User(object):
  def fullname(self):
    return "{} {}".format(self.firstName, self.lastName)
```
With this class definition, the full name can be obtained with the function call.
```
user = User({"firstName": "Alyssa", "lastName": "Yssa"})
print(user.fullname())
# Alyssa Yssa
```

### Avro Schema with Validation
In some use cases, some extra validation is required, for example:
**Date.avsc**
```
{
  "name": "Date",
  "type": "record",
  "fields": [
    {
      "name": "year",
      "type": "int"
    },
    {
      "name": "month",
      "type": "int"
    },
    {
      "name": "day",
      "type": "int"
    }
  ]
}
```
The _month_ and _day_ of a date cannot be arbitrary integers. A extra validation can be done as following:
```
@avro_schema(EXAMPLE_NAMES, schema_file=os.path.join(DIRNAME, "Date.avsc"))
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
    return super().validate(data) \
        and datetime.date(data['year'], data['month'], data['day'])
```
The `Date` class can validate the input before assign it to then underlying avro schema
```
date = Date({"year": 2018, "month": 12, "date": 99})
# ValueError: day is out of range for month
```

## Contributing
After cloning/forking the repo, navigate to the directory and run
```
source init.sh
```
The python environment should be ready for you.

## Authors

* **Kun Fang** - (https://github.com/kun-fang)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

