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

from argparse import ArgumentParser, Namespace, RawTextHelpFormatter
from time import sleep

from rich.progress import track


def create_cmd_line_args_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Parallel search of prime numbers", formatter_class=RawTextHelpFormatter)

    # positional mandatory arguments
    parser.add_argument("iterations",
        help = "the number of iterations to be performed",
        type=int)
    parser.add_argument("sleep_seconds",
        help = "the number of seconds to sleep between iterations",
        type=int)

    return parser


def parse_cmd_line_args() -> Namespace:
    parser = create_cmd_line_args_parser()
    params = parser.parse_args()
    return params


def main() -> None:
    cmd_line_args = parse_cmd_line_args()
    for _ in track(range(cmd_line_args.iterations)):
        sleep(cmd_line_args.sleep_seconds)


if __name__ == "__main__":
    main()
