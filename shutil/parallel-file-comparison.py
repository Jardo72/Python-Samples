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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from os import walk
from pathlib import Path
from pprint import pprint
from time import perf_counter
from traceback import print_exc
from zlib import crc32


MAX_WORKERS = 16


class InvalidConfigurationError(Exception):
    ...


@dataclass(frozen=True)
class Configuration:
    source_path: str
    destination_path: str
    workers: int

    def __post_init__(self) -> None:
        if not Path(self.source_path).is_dir():
            raise InvalidConfigurationError(f"Source path '{self.source_path}' is not a valid directory.")
        if not Path(self.destination_path).is_dir():
            raise InvalidConfigurationError(f"Destination path '{self.destination_path}' is not a valid directory.")
        if self.source_path == self.destination_path:
            raise InvalidConfigurationError("Source and destination paths must be different.")


@dataclass(frozen=True)
class Request:
    path: str
    files: tuple[str, ...]


@dataclass(frozen=True)
class FileChecksum:
    filename: str
    checksum: str


def calculate_crc32(filepath: str) -> str:
    checksum = 0
    with open(filepath, "rb") as file:
        for chunk in iter(lambda: file.read(256 * 1024), b""):
            checksum = crc32(chunk, checksum)
    return hex(checksum & 0xFFFFFFFF)  # Ensure the result is unsigned


def process_request(request: Request) -> tuple[FileChecksum, ...]:
    result = []
    for file in request.files:
        file_path = Path(request.path) / file
        if file_path.is_file():
            crc32_value = calculate_crc32(str(file_path))
            result.append(FileChecksum(
                filename=str(file_path),
                checksum=crc32_value
            ))
    return tuple(result)


class CRC32Collector:

    def __init__(self, root_path: str, max_workers: int) -> None:
        self._root_path = root_path
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

    def collect(self) -> tuple[FileChecksum, ...]:
        future_list = []
        for dir, _, files in walk(self._root_path, topdown=True):
            request = Request(
                path=dir,
                files=tuple(files)
            )
            future = self._executor.submit(process_request, request)
            future_list.append(future)

        result = []
        for future in future_list:
            try:
                result.extend(future.result())
            except Exception as e:
                print(f"An error occurred while processing: {e}")
                print_exc()
        return tuple(result)


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
This script compares the files in the given source directory structure with the files
in the given destination directory structure. The comparison is recursive and parallel,
with configurable number of worker processes per directory structure. For each file,
the script checks if the file exists in both directories and if the contents are identical.
The comparison of the contents is done using a hash function to ensure that the files are
identical.
"""


def create_cmd_line_args_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Parallel Comparison of Files in Directory Structures",
        formatter_class=RawTextHelpFormatter,
        epilog=epilog()
    )

    # positional mandatory arguments
    parser.add_argument("source_path",
        help = "path to the source directory for the comparison",
        type=str)
    parser.add_argument("destination_path",
        help = "path to the destination directory for the comparison",
        type=str)

    # optional arguments
    parser.add_argument("-w", "--workers",
        dest="workers",
        default=4,
        help=f"optional number of worker threads to be used (default = 4, max = {MAX_WORKERS})",
        type=int)

    return parser


def parse_cmd_line_args() -> Configuration:
    parser = create_cmd_line_args_parser()
    params = parser.parse_args()
    return Configuration(
        source_path=params.source_path,
        destination_path=params.destination_path,
        workers=params.workers,
    )


def format_duration(duration_millis: int) -> str:
    duration_sec = round(duration_millis / 1000)
    hours, remainder = divmod(duration_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"


def main() -> None:
    try:
        config = parse_cmd_line_args()
        crc_collector = CRC32Collector(config.source_path, config.workers)
        file_checksum_list = crc_collector.collect()
        print()
        print(f"Collected {len(file_checksum_list)} file checksums from '{config.source_path}'.")
        pprint(file_checksum_list)
        print(f"Collected {len(file_checksum_list)} file checksums from '{config.source_path}'.")
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
