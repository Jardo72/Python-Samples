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
from asyncio import create_task, run, sleep
from dataclasses import dataclass
from math import isqrt
from pprint import pprint

from colorama import init, Fore


@dataclass(frozen=True)
class PerfectNumber:
    number: int
    divisors: tuple[int, ...]


@dataclass(frozen=True)
class TestResult:
    prime_number: tuple[int, ...]
    perfect_number: tuple[int, ...]


def epilog() -> str:
    return """
This script finds prime and perfect numbers in a given range. Both search tasks run concurrently
using asyncio. During the search, each task will periodically pause for a short time to simulate
a long-running task. Whenever sleeping, each of the tasks will print a message indicating that it
is waiting. For educational purposes, the script uses colorama to color the output of the waiting
messages. Each task uses a different color so the user can easily distinguish between them.
"""


def create_cmd_line_args_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Parallel search of perfect numbers",
        formatter_class=RawTextHelpFormatter,
        epilog=epilog(),
    )

    # positional mandatory arguments
    parser.add_argument("start",
        help = "the start of the range of integers in which perfect numbers are to be searched",
        type=int)
    parser.add_argument("end",
        help = "the end of the range of integers in which perfect numbers are to be searched",
        type=int)

    # optional arguments
    parser.add_argument("-m", "--max-results",
        dest="max_results",
        help="optional limit for the number of results returned by the search",
        type=int)

    return parser


def parse_cmd_line_args() -> Namespace:
    parser = create_cmd_line_args_parser()
    params = parser.parse_args()
    if params.start < 1 or params.end < 1:
        parser.error("Both start and end must be positive integers.")
    if params.start >= params.end:
        parser.error("Start must be less than end.")
    if params.max_results and params.max_results < 1:
        parser.error("Max results must be a positive integer.")
    return params


async def find_prime_numbers(start: int, end: int, max_results: int | None) -> tuple[int, ...]:
    result = []
    for value in range(start, end + 1):
        if value % 50 == 0:
            print(f"{Fore.GREEN}Prime number search is yielding control...")
            await sleep(0)
        if is_prime_number(value):
            result.append(value)
            if max_results and len(result) >= max_results:
                break
    return tuple(result)


def is_prime_number(value: int) -> bool:
    if value < 2:
        return False
    if value == 2:
        return True
    if value % 2 == 0:
        return False
    for divisor in range(3, isqrt(value) + 1, 2):
        if value % divisor == 0:
            return False
    return True


async def find_perfect_numbers(start: int, end: int, max_results: int | None) -> tuple[int, ...]:
    result = []
    for value in range(start, end + 1):
        if value % 1000 == 0:
            print(f"{Fore.BLUE}Perfect number search is yielding control...")
            await sleep(0)
        if is_perfect_number(value):
            result.append(value)
            if max_results and len(result) >= max_results:
                break
    return tuple(result)


def is_perfect_number(value: int) -> PerfectNumber | None:
    divisors = []
    for divisor in range(1, value // 2 + 1):
        if value % divisor == 0:
            divisors.append(divisor)
    if sum(divisors) == value:
        return PerfectNumber(number=value, divisors=tuple(divisors))
    else:
        return None


async def invoke_tests(start: int, end: int, max_results: int | None) -> TestResult:
    prime_number_task = create_task(find_prime_numbers(start, end, max_results))
    perfect_number_task = create_task(find_perfect_numbers(start, end, max_results))

    prime_number_result = await prime_number_task
    perfect_number_result = await perfect_number_task

    print()
    print(f"{Fore.GREEN}Prime task status - done={prime_number_task.done()}, cancelled={prime_number_task.cancelled()}")
    print(f"{Fore.BLUE}Perfect task status - done={perfect_number_task.done()}, cancelled={perfect_number_task.cancelled()}")
    print()

    return TestResult(
        prime_number=prime_number_result,
        perfect_number=perfect_number_result,
    )


def main() -> None:
    init(autoreset=True)
    cmd_line_args = parse_cmd_line_args()
    print(f"Searching for prime and perfect numbers in the range between {cmd_line_args.start} to {cmd_line_args.end}...")
    print()
    test_result = run(invoke_tests(cmd_line_args.start, cmd_line_args.end, cmd_line_args.max_results))
    pprint(test_result)


if __name__ == "__main__":
    main()
