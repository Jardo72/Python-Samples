#
# Copyright 2025 Jaroslav Chmurny
#
# This file is part of Python Samples.
#
# Python Samples is free software. It is licensed under the Apache License,
# Version 2.0 # (the "License"); you may not use this file except
# in compliance with the # License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from dataclasses import dataclass
from pprint import pprint
from typing import List

import httpx
from marshmallow import Schema, fields, post_load


@dataclass(frozen=True)
class Geo:
    lat: str
    lng: str


@dataclass(frozen=True)
class Address:
    street: str
    suite: str
    city: str
    zip_code: str
    geo: Geo


@dataclass(frozen=True)
class Company:
    name: str
    catch_phrase: str
    bs: str


@dataclass(frozen=True)
class User:
    id: int
    name: str
    username: str
    email: str
    address: Address
    phone: str
    website: str
    company: Company


class GeoSchema(Schema):
    lat = fields.Str()
    lng = fields.Str()

    @post_load
    def make_geo(self, data, **kwargs):
        return Geo(**data)

class AddressSchema(Schema):
    street = fields.Str()
    suite = fields.Str()
    city = fields.Str()
    zip_code = fields.Str(data_key="zipcode")
    geo = fields.Nested(GeoSchema)

    @post_load
    def make_address(self, data, **kwargs):
        return Address(**data)

class CompanySchema(Schema):
    name = fields.Str()
    catch_phrase = fields.Str(data_key="catchPhrase")
    bs = fields.Str()

    @post_load
    def make_company(self, data, **kwargs):
        return Company(**data)

class UserSchema(Schema):
    id = fields.Int()
    name = fields.Str()
    username = fields.Str()
    email = fields.Str()
    address = fields.Nested(AddressSchema)
    phone = fields.Str()
    website = fields.Str()
    company = fields.Nested(CompanySchema)

    @post_load
    def make_user(self, data, **kwargs):
        return User(**data)


class RestApiError(Exception):
    ...


class RestApiClient:

    _BASE_URL = "https://jsonplaceholder.typicode.com"

    def __init__(self):
        ...

    def get_users(self) -> List[User]:
        response = httpx.get(f"{self._BASE_URL}/users")
        if response.status_code != 200:
            raise RestApiError(f"Failed to fetch users: {response.status_code}")
        response_data = response.json()
        return [UserSchema().load(user_data) for user_data in response_data]


def main() -> None:
    rest_api_client = RestApiClient()
    users = rest_api_client.get_users()
    pprint(users)


if __name__ == "__main__":
    main()
