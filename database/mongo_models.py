from mongoengine import (
    Document,
    StringField,
    IntField,
    FloatField,
    BooleanField,
    DateTimeField,
    ReferenceField
)


class Car(Document):
    url = StringField(max_length=255, required=True)
    title = StringField(max_length=255, required=True)
    price_usd = FloatField(required=True)
    odometer = FloatField(required=True)
    username = StringField(max_length=255, required=True)
    phone_number = StringField(max_length=255, required=True)
    image_url = StringField(max_length=255, required=False)
    images_count = IntField(required=True)
    car_number = StringField(max_length=255, required=False)
    car_vin = StringField(max_length=255, required=True, unique=True)
    datetime_found = DateTimeField(required=True)


class Task(Document):
    page_number = IntField(required=True)
    in_work = BooleanField(default=False)
    completed = BooleanField(default=False)


class Result(Document):
    task = ReferenceField("Task", reverse_delete_rule=1)
    car = ReferenceField("Car", reverse_delete_rule=1)
