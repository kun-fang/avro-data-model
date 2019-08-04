import datetime
import traceback

from example_models import Date, User, Name, Employee


if __name__ == '__main__':
    item = {"year": 1985, "month": 4, "day": 3}
    date = Date(item)

    item = datetime.datetime(2017, 7, 20)
    date = Date(item)

    data = {
        'favorite_color': None,
        'favorite_number': 256,
        'firstName': 'Alyssa',
        'lastName': 'Yssa',
        'occupation': 'STUDENT',
        'birthday': {
            'year': 1956,
            'month': 4,
            'day': 3
        }
    }
    user = User(data)

    print(user.fullname())
    print(repr(user))
    print(user.json_dumps())

    user.birthday = date
    print(user)

    user.birthday = {
        'year': 2000,
        'month': 12,
        'day': 12
    }
    print(user)

    try:
        user.birthday = {
            'year': 2000,
            'month': 12,
            'day': 40
        }
        print(user)
    except:
        traceback.print_exc()

    name = Name({'firstName': 'Alyssa', 'lastName': 'Yssa'})
    employee = Employee({"id": "1", "name": name.to_dict()})
    print(name)
    print(employee)
