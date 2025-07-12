from argparse import ArgumentParser, Namespace, RawTextHelpFormatter
from configparser import ConfigParser
from dataclasses import dataclass
from os import environ

from openai import OpenAI


class MissingAPIKeyError(Exception):
    ...


@dataclass(frozen=True)
class Configuration:
    api_base: str
    api_key: str
    model: str
    role: str


def epilog() -> str:
    return """
This script requires an OpenAI API key to function.
Set the environment variable 'OPENAI_API_KEY' with your API key.
The configuration file should be in INI format with the following structure:

[OpenAI]
api_base = https://api.openai.com/v1
model = text-davinci-002
role = user
"""


def parse_cmd_line_args() -> Namespace:
    parser = ArgumentParser(
        description="OpenAI API client",
        formatter_class=RawTextHelpFormatter,
        epilog=epilog(),
    )
    parser.add_argument(
        "config_file",
        type=str,
        help="Path to the configuration file containing API settings (INI format expected).",
    )
    return parser.parse_args()


def read_api_key() -> str:
    if "OPENAI_API_KEY" not in environ:
        raise MissingAPIKeyError("Environment variable 'OPENAI_API_KEY' is not set.")
    result = environ["OPENAI_API_KEY"]
    if not result:
        raise MissingAPIKeyError("Environment variable 'OPENAI_API_KEY' is empty.")
    return result


def read_configuration(filename: str) -> Configuration:
    config_parser = ConfigParser()
    config_parser.read(filename)

    api_key = read_api_key()

    return Configuration(
        api_base=config_parser.get("OpenAI", "api_base"),
        api_key=api_key,
        model=config_parser.get("OpenAI", "model"),
        role=config_parser.get("OpenAI", "role"),
    )


def main() -> None:
    try:
        cmd_line_args = parse_cmd_line_args()
        configuration = read_configuration(cmd_line_args.config_file)

        openai_client = OpenAI(api_key=configuration.api_key) # , api_base=configuration.api_base
        response = openai_client.responses.create(
            model=configuration.model,
            input="Do you believe this is my first API request using OpenAI?",
        )

        print(response)
    except MissingAPIKeyError as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
