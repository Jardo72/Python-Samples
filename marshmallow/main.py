from argparse import ArgumentParser, Namespace, RawTextHelpFormatter
from dataclasses import dataclass
from datetime import date
from json import load as load_json
from typing import Any, Dict

from marshmallow_dataclass import class_schema
from yaml import safe_load as load_yaml


@dataclass(frozen=True)
class Address:
    street: str
    city: str
    zip_code: str
    country: str


@dataclass(frozen=True)
class Person:
    first_name: str
    last_name: str
    birth_date: date
    address: Address


PersonSchema = class_schema(Person)()
AddressSchema = class_schema(Address)()


def create_command_line_arguments_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Marshmallow demo", formatter_class=RawTextHelpFormatter)

    # positional mandatory arguments
    parser.add_argument("input_file",
        help = "the name of the input file containing the JSON or YAML data to be read")

    return parser


def parse_command_line_arguments() -> Namespace:
    parser = create_command_line_arguments_parser()
    params = parser.parse_args()
    return params


def read_json_file(filename: str) -> Dict[str, Any]:
    with open(filename, "r") as json_file:
        return load_json(json_file)


def read_yaml_file(filename: str) -> Dict[str, Any]:
    with open(filename, "r") as yaml_file:
        return load_yaml(yaml_file)


def read_file(filename: str) -> Person:
    if filename.endswith(".json"):
        data_as_dict = read_json_file(filename)
    elif filename.endswith(".yaml") or filename.endswith(".yml"):
        data_as_dict = read_yaml_file(filename)
    else:
        raise ValueError(f"Unsupported file format: {filename}. Use .json or .yaml/.yml.")
    return PersonSchema.load(data_as_dict)


def main() -> None:
    command_line_arguments = parse_command_line_arguments()
    person = read_file(command_line_arguments.input_file)
    print(person)


if __name__ == "__main__":
    main()
