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
from argparse import ArgumentParser, RawTextHelpFormatter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from re import compile, IGNORECASE
from shutil import copytree
from time import perf_counter
from traceback import print_exc
from typing import Optional


MAX_WORKERS = 16


class InvalidConfigurationError(Exception):
    ...


@dataclass(frozen=True)
class Configuration:
    source_path: str
    destination_path: str
    regex_filter: Optional[str]
    workers: int
    dry_run: bool

    def __post_init__(self) -> None:
        if not Path(self.source_path).is_dir():
            raise InvalidConfigurationError(f"Source path '{self.source_path}' is not a valid directory.")
        if self.source_path == self.destination_path:
            raise InvalidConfigurationError("Source and destination paths must be different.")
        if not 1 <= self.workers <= MAX_WORKERS:
            raise InvalidConfigurationError(f"Number of workers must be a positive integer between 1 and {MAX_WORKERS}")


@dataclass(frozen=True)
class CopyRequest:
    source: str
    destination: str


@dataclass(frozen=True)
class CopyResult:
    request: CopyRequest
    duration_millis: int
    exception: Optional[Exception] = None


@dataclass(frozen=True)
class Summary:
    overall_duration_millis: int
    success_count: int
    failure_count: int


class Stopwatch:

    def __init__(self) -> None:
        self._start_time = perf_counter()

    @staticmethod
    def start() -> Stopwatch:
        return Stopwatch()

    def elapsed_time_millis(self) -> int:
        duration = perf_counter() - self._start_time
        return int(1000 * duration)


def epilog() -> str:
    return """
This script copies the given directory structure to the specified destination.
The copying is recursive and parallel, with configurable number of worker threads.
If the destination directory does not exist, the script will create it.
Optionally, it supports filtering subdirectories using a regex pattern. Subdirectories
not matching the regex filter will be skipped. The filter is case-insensitive.
"""


def create_cmd_line_args_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Parallel Copying of Directory Structures",
        formatter_class=RawTextHelpFormatter,
        epilog=epilog()
    )

    # positional mandatory arguments
    parser.add_argument("source_path",
        help = "path to the source directory to be copied",
        type=str)
    parser.add_argument("destination_path",
        help = "path to the destination directory where the source directory is to be copied",
        type=str)

    # optional arguments
    parser.add_argument("-f", "--regex-filter",
        dest="regex_filter",
        help="optional regex pattern (case-insensitive) for filtering subdirectories to copy (default = all subdirectories)",
        type=str)
    parser.add_argument("-w", "--workers",
        dest="workers",
        default=4,
        help=f"optional number of worker threads to be used (default = 4, max = {MAX_WORKERS})",
        type=int)
    parser.add_argument("-d", "--dry-run",
        dest="dry_run",
        default=False,
        action="store_true",
        help="if specified, the script will not copy any files (it will only print what would be copied)"
    )

    return parser


def parse_cmd_line_args() -> Configuration:
    parser = create_cmd_line_args_parser()
    params = parser.parse_args()
    return Configuration(
        source_path=params.source_path,
        destination_path=params.destination_path,
        regex_filter=params.regex_filter,
        workers=params.workers,
        dry_run=params.dry_run,
    )


def get_sorted_subdirs(config: Configuration) -> tuple[str, ...]:
    ignored_count = 0
    result = []

    regex = compile(config.regex_filter, IGNORECASE) if config.regex_filter else None
    for path in Path(config.source_path).iterdir():
        if not path.is_dir():
            continue
        if regex is not None and not regex.match(path.name):
                ignored_count += 1
                continue
        result.append(str(path))

    print()
    print(f"{len(result)} relevant subdirectories found in '{config.source_path}'")
    if ignored_count > 0:
        print(f"{ignored_count} subdirectories ignored due to regex filter '{config.regex_filter}'")
    print()

    return tuple(sorted(result))


def copy_subdir(request: CopyRequest, dry_run: bool) -> CopyResult:
    print(f"Going to copy '{request.source}' to '{request.destination}")
    stopwatch = Stopwatch.start()
    try:
        if not dry_run:
            copytree(request.source, request.destination, dirs_exist_ok=True)
        duration_millis = stopwatch.elapsed_time_millis()
        return CopyResult(
            request=request,
            duration_millis=duration_millis
        )
    except Exception as e:
        return CopyResult(
            request=request,
            duration_millis=stopwatch.elapsed_time_millis(),
            exception=e
        )


def copy_subdirs(config: Configuration, source_list: tuple[str, ...]) -> Summary:
    Path(config.destination_path).mkdir(parents=True, exist_ok=True)
    stopwatch = Stopwatch()
    future_list = []

    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        for source in source_list:
            destination = Path(config.destination_path) / Path(source).name
            request = CopyRequest(
                source=source,
                destination=str(destination)
            )
            future = executor.submit(copy_subdir, request, config.dry_run)
            future_list.append(future)
        
        success_count = 0
        failure_count = 0
        for future in future_list:
            result = future.result()
            if result.exception is None:
                formatted_duration = format_duration(result.duration_millis)
                print(f"Successfully copied '{result.request.source}' to '{result.request.destination}' in {result.duration_millis} ms ({formatted_duration})")
                success_count += 1
            else:
                print(f"Failed to copy '{result.request.source}' to '{result.request.destination}'")
                print(str(result.exception))
                failure_count += 1

        return Summary(
            overall_duration_millis=stopwatch.elapsed_time_millis(),
            success_count= success_count,
            failure_count=failure_count
        )


def format_duration(duration_millis: int) -> str:
    duration_sec = round(duration_millis / 1000)
    hours, remainder = divmod(duration_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"


def print_summary(config: Configuration, summary: Summary) -> None:
    print()
    formatted_duration = format_duration(summary.overall_duration_millis)
    print(f"Source path:                        {config.source_path}")
    print(f"Destination path:                   {config.destination_path}")
    print(f"Workers:                            {config.workers}")
    print(f"Overall duration:                   {summary.overall_duration_millis} ms ({formatted_duration})")
    print(f"Successfully copied subdirectories: {summary.success_count}")
    print(f"Failed subdirectories:              {summary.failure_count}")
    print(f"Dry run:                            {'yes' if config.dry_run else 'no'}")
    print()


def main() -> None:
    try:
        config = parse_cmd_line_args()
        source_list = get_sorted_subdirs(config)
        if not source_list:
            print("No relevant subdirectories found - nothing to copy.")
            return
        summary = copy_subdirs(config, source_list)
        print_summary(config, summary)
    except InvalidConfigurationError as e:
        print("ERROR!!! Invalid command line arguments.")
        print(str(e))
        exit(1)
    except Exception as e:
        print("ERROR!!! An unexpected exception has occurred.")
        print_exc()
        exit(1)


if __name__ == "__main__":
    main()
