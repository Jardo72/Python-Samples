from dataclasses import dataclass
from typing import List

import httpx


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
        return []



def main() -> None:
    pass


if __name__ == "__main__":
    main()
