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

from __future__ import annotations
from argparse import ArgumentParser, Namespace, RawTextHelpFormatter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from math import isqrt
from multiprocessing import cpu_count, current_process
from time import perf_counter
from typing import List


@dataclass(frozen=True)
class SearchRange:
    min: int
    max: int


@dataclass(frozen=True)
class SearchResult:
    process: str
    duration_millis: int
    search_range: SearchRange
    prime_numbers: List[int]


class Stopwatch:

    def __init__(self) -> None:
        self.start_time = perf_counter()

    @staticmethod
    def start() -> Stopwatch:
        return Stopwatch()


    def elapsed_time_millis(self) -> int:
        duration = perf_counter() - self.start_time
        return int(1000 * duration)


def epilog() -> str:
    return """
This script finds all prime numbers in the given range using parallel processing.
Depending on the specified executor type, it uses either multithreading or multiprocessing.
The prime numbers are written to the specified output file.
"""


def create_command_line_arguments_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Parallel search of prime numbers", formatter_class=RawTextHelpFormatter, epilog=epilog())

    # positional mandatory arguments
    parser.add_argument("start",
        help = "the start of the range of integers in which prime numbers are to be searched",
        type=int)
    parser.add_argument("end",
        help = "the end of the range of integers in which prime numbers are to be searched",
        type=int)
    parser.add_argument("output_file",
        help = "the name of the output file the prime numbers are to be written to",
        type=str)

    # optional arguments
    parser.add_argument("-b", "--bulk_size",
        dest="bulk_size",
        default=5000,
        help="optional number of values to be tested by a single executor task (default = 5000)",
        type=int)

    parser.add_argument("-e", "--executor",
        dest="executor",
        choices=["thread", "process"],
        default="thread",
        help="allows to specify whether multithreading or multiprocessing is to be used (default = thread)",
        type=str)

    parser.add_argument("-w", "--workers",
        dest="workers",
        default=cpu_count(),
        help="optional number of workers (threads or processes) to be used (default = number of available CPU cores)",
        type=int)

    return parser


def parse_command_line_arguments() -> Namespace:
    parser = create_command_line_arguments_parser()
    params = parser.parse_args()
    return params


def print_request(start: int, end: int, bulk_size: int, workers: int, executor: str) -> None:
    print()
    print(f"Range start:  {start}")
    print(f"Range end:    {end}")
    print(f"Bulk size:    {bulk_size}")
    print(f"Workers:      {workers}")
    print(f"Executor:     {executor}")
    print()


def current_process_name() -> str:
    process = current_process()
    return process.name


def create_executor(executor: str, max_workers: int) -> ThreadPoolExecutor | ProcessPoolExecutor:
    if executor == "process":
        return ProcessPoolExecutor(max_workers=max_workers)
    elif executor == "thread":
        return ThreadPoolExecutor(max_workers=max_workers)
    else:
        raise ValueError(f"Unknown executor type: {executor}. Use 'thread' or 'process'.")


def is_prime_number(value: int) -> bool:
    if value % 2 == 0:
        return False
    for divider in range(3, isqrt(value) + 1, 2):
        if value % divider == 0:
            return False
    return True


def get_prime_numbers(search_range: SearchRange) -> SearchResult:
    stopwatch = Stopwatch()
    prime_numbers = []

    for value in range(search_range.min, search_range.max + 1):
        if is_prime_number(value):
            prime_numbers.append(value)

    return SearchResult(
        process=current_process_name(),
        duration_millis=stopwatch.elapsed_time_millis(),
        search_range=search_range,
        prime_numbers=prime_numbers
    )


def write_to_file(prime_numbers: List[int], filename: str) -> None:
    with open(filename, "w") as output_file:
        for number in prime_numbers:
            output_file.write(f"{number}\n")


def format_duration(duration_sec: float) -> str:
    duration_sec = round(duration_sec)
    hours, remainder = divmod(duration_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"


def main() -> None:
    command_line_arguments = parse_command_line_arguments()
    print_request(
        command_line_arguments.start,
        command_line_arguments.end,
        command_line_arguments.bulk_size,
        command_line_arguments.workers,
        command_line_arguments.executor
    )
    future_list = []
    
    with create_executor(command_line_arguments.executor, command_line_arguments.workers) as executor:
        stopwatch = Stopwatch()
        start = command_line_arguments.start
        while start <= command_line_arguments.end:
            end = min(start + command_line_arguments.bulk_size - 1, command_line_arguments.end)
            future = executor.submit(get_prime_numbers, SearchRange(start, end))
            future_list.append(future)
            start += command_line_arguments.bulk_size

        prime_numbers = []
        for future in future_list:
            search_result = future.result()
            prime_numbers += search_result.prime_numbers

        duration_millis = stopwatch.elapsed_time_millis()
        write_to_file(prime_numbers, command_line_arguments.output_file)
        print(f"Overall number of prime numbers found: {len(prime_numbers)}")
        print(f"Overall search duration:               {duration_millis} ms ({format_duration(duration_millis / 1000)})")
        print(f"Batch count:                           {len(future_list)}")


if __name__ == "__main__":
    main()
