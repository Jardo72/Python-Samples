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
from shutil import copytree
from time import perf_counter, sleep


@dataclass(frozen=True)
class Configuration:
    source_path: str
    destination_path: str
    range_start: int | None
    range_end: int | None
    workers: int | None

    def __post_init__(self) -> None:
        if self.range_start is not None and self.range_start < 0:
            raise ValueError("range_start must be a non-negative integer")
        if self.range_end is not None and self.range_end < 0:
            raise ValueError("range_end must be a non-negative integer")
        if self.workers is not None and not 1 <= self.workers <= 10:
            raise ValueError("workers must be a positive integer")


@dataclass(frozen=True)
class CopyRequest:
    source: str
    destination: str


@dataclass(frozen=True)
class CopyResult:
    request: CopyRequest
    duration_millis: int
    exception: Exception | None = None


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
This script copies a directory structure in parallel, allowing for optional
specification of a range of subdirectories to copy and the number of workers
to use. The source and destination paths must be specified, and the range
of subdirectories can be limited using the --range-start and --range-end options.
The specified source directory is copied recursively to the destination directory.
"""


def create_cmd_line_args_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Parallel Copying of Directory Structures", formatter_class=RawTextHelpFormatter, epilog=epilog())

    # positional mandatory arguments
    parser.add_argument("source_path",
        help = "path to the source directory to be copied",
        type=str)
    parser.add_argument("destination_path",
        help = "path to the destination directory where the source directory is to be copied",
        type=str)

    # optional arguments
    parser.add_argument("-s", "--range-start",
        dest="range_start",
        help="optional start index in alphabetically orderered list of subdirectories to be copied (default = first subdirectory)",
        type=int)
    parser.add_argument("-e", "--range-end",
        dest="range_end",
        help="optional end index in alphabetically orderered list of subdirectories to be copied (default = last subdirectory)",
        type=int)
    parser.add_argument("-w", "--workers",
        dest="workers",
        default=4,
        help="optional number of workers (threads or processes) to be used (default = 4)",
        type=int)

    return parser


def parse_cmd_line_args() -> Configuration:
    parser = create_cmd_line_args_parser()
    params = parser.parse_args()
    return Configuration(
        source_path=params.source_path,
        destination_path=params.destination_path,
        range_start=params.range_start,
        range_end=params.range_end,
        workers=params.workers
    )


def get_sorted_subdirs(directory: str) -> tuple[str, ...]:
    result = [str(p) for p in Path(directory).iterdir() if p.is_dir()]
    return tuple(sorted(result))


def copy_subdir(request: CopyRequest) -> CopyResult:
    print(f"Going to copy {request.source} to {request.destination}")
    stopwatch = Stopwatch.start()
    try:
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


def copy_subdirs(config: Configuration, source_list: tuple[str, ...]) -> None:
    Path(config.destination_path).mkdir(parents=True, exist_ok=True)
    executor = ThreadPoolExecutor(max_workers=config.workers)
    stopwatch = Stopwatch()
    future_list = []

    for source in source_list:
        destination = Path(config.destination_path) / Path(source).name
        request = CopyRequest(
            source=source,
            destination=str(destination)
        )
        future = executor.submit(copy_subdir, request)
        future_list.append(future)
    
    success_count = 0
    failure_count = 0
    for future in future_list:
        result = future.result()
        if result.exception is None:
            print(f"Successfully copied {result.request.source} to {result.request.destination} in {result.duration_millis} ms")
            success_count += 1
        else:
            print(f"Failed to copy {result.request.source} to {result.request.destination}")
            print(str(result.exception))
            failure_count += 1

    duration_millis = stopwatch.elapsed_time_millis()
    print(f"Overall duration:  {duration_millis} ms ({format_duration(duration_millis / 1000)})")
    print(f"Successful copies: {success_count}")
    print(f"Failed copies:     {failure_count}")


def format_duration(duration_sec: float) -> str:
    duration_sec = round(duration_sec)
    hours, remainder = divmod(duration_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"


def main() -> None:
    config = parse_cmd_line_args()
    source_list = get_sorted_subdirs(config.source_path)
    copy_subdirs(config, source_list)


if __name__ == "__main__":
    main()
