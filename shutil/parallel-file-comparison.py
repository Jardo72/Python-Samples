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

from argparse import ArgumentParser, RawTextHelpFormatter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from json import dump
from math import ceil
from os import cpu_count, walk
from pathlib import Path
from traceback import print_exc
from zlib import crc32

from commons import format_duration, Stopwatch


@dataclass(frozen=True)
class Configuration:
    source_path: str
    destination_path: str
    diff_details_report: str

    @property
    def worker_count_per_collector(self) -> int:
        cpu_count_value = cpu_count()
        if cpu_count_value is None or cpu_count_value <= 1:
            return 1
        return ceil(cpu_count_value / 2)


@dataclass(frozen=True)
class Request:
    root_path: str
    path: str
    files: tuple[str, ...]


@dataclass(frozen=True)
class FileChecksum:
    root_path: str
    filename: str
    checksum: str

    @property
    def relative_path(self) -> str:
        return Path(self.filename).relative_to(self.root_path).as_posix()


@dataclass(frozen=True)
class ChecksumDiscrepancy:
    source_checksum: FileChecksum
    destination_checksum: FileChecksum


@dataclass(frozen=True)
class ComparisonResult:
    files_missing_in_source: tuple[str, ...]
    files_missing_in_destination: tuple[str, ...]
    checksum_discrepancies: tuple[ChecksumDiscrepancy, ...]
    duration_millis: int
    number_of_files_in_source: int = 0
    number_of_files_in_destination: int = 0

    @property
    def number_of_files_missing_in_source(self) -> int:
        return len(self.files_missing_in_source)

    @property
    def number_of_files_missing_in_destination(self) -> int:
        return len(self.files_missing_in_destination)

    @property
    def number_of_checksum_discrepancies(self) -> int:
        return len(self.checksum_discrepancies)


class CRC32Collector:

    def __init__(self, root_path: str, worker_count: int) -> None:
        self._root_path = root_path
        self._executor = ProcessPoolExecutor(max_workers=worker_count)

    def collect(self) -> tuple[FileChecksum, ...]:
        future_list = []
        for dir, _, files in walk(self._root_path, topdown=True):
            request = Request(
                root_path=self._root_path,
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

    def __del__(self) -> None:
        self._executor.shutdown(wait=True)


class Comparator:

    def __init__(self, config: Configuration) -> None:
        self._source_crc_collector = CRC32Collector(config.source_path, config.worker_count_per_collector)
        self._destination_crc_collector = CRC32Collector(config.destination_path, config.worker_count_per_collector)

    @staticmethod
    def _convert_to_dict(checksums: tuple[FileChecksum, ...]) -> dict[str, FileChecksum]:
        return {file_checksum.relative_path: file_checksum for file_checksum in checksums}

    def compare(self) -> ComparisonResult:
        with ThreadPoolExecutor(max_workers=2) as executor:
            stopwatch = Stopwatch.start()
            source_future = executor.submit(lambda: self._source_crc_collector.collect())
            destination_future = executor.submit(lambda: self._destination_crc_collector.collect())
        duration_millis = stopwatch.elapsed_time_millis()
        source_checksums = self._convert_to_dict(source_future.result())
        destination_checksums = self._convert_to_dict(destination_future.result())
        files_missing_in_source = tuple(
            file for file in destination_checksums if file not in source_checksums
        )
        files_missing_in_destination = tuple(
            file for file in source_checksums if file not in destination_checksums
        )
        checksum_discrepancies = tuple(
            ChecksumDiscrepancy(source_checksum, destination_checksum)
            for rel_path, source_checksum in source_checksums.items()
            if (destination_checksum := destination_checksums.get(rel_path)) and
               source_checksum.checksum != destination_checksum.checksum
        )
        return ComparisonResult(
            number_of_files_in_source=len(source_checksums),
            number_of_files_in_destination=len(destination_checksums),
            files_missing_in_source=files_missing_in_source,
            files_missing_in_destination=files_missing_in_destination,
            checksum_discrepancies=checksum_discrepancies,
            duration_millis=duration_millis,
        )


def epilog() -> str:
    return """
This script compares the files in the given source directory structure with the files
in the given destination directory structure. The comparison is recursive and parallel.
For each file, the script checks if the file exists in both directories and if the
contents are identical. The comparison of the contents is done using a hash function
to ensure that the files are identical. After the comparison, the script generates a
detailed report of the differences found in form of a JSON file. In addition, it
prints a summary of the comparison to the console.
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
    parser.add_argument("diff_details_report",
        help="filename of the output diff details report",
        type=str)

    return parser


def parse_cmd_line_args() -> Configuration:
    parser = create_cmd_line_args_parser()
    params = parser.parse_args()
    if not Path(params.source_path).is_dir():
        parser.error(f"Source path '{params.source_path}' is not a valid directory.")
    if not Path(params.destination_path).is_dir():
        parser.error(f"Destination path '{params.destination_path}' is not a valid directory.")
    if params.source_path == params.destination_path:
        parser.error("Source and destination paths must be different.")
    return Configuration(
        source_path=params.source_path,
        destination_path=params.destination_path,
        diff_details_report=params.diff_details_report,
    )


def print_prestart_info(config: Configuration) -> None:
    current_timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z (%z)")
    print()
    print(f"Going to compare '{config.source_path}' with '{config.destination_path}'")
    print(f"{cpu_count()} CPU core(s) detected, {config.worker_count_per_collector} worker(s) per collector will be used")
    print(f"Start time {current_timestamp}")
    print()


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
                root_path=request.root_path,
                filename=str(file_path),
                checksum=crc32_value,
            ))
    return tuple(result)


def write_json_report(comparison_result: ComparisonResult, filename: str) -> None:
    with open(filename, 'w', encoding='utf-8') as file:
        report_data = {
            "files_missing_in_source": comparison_result.files_missing_in_source,
            "files_missing_in_destination": comparison_result.files_missing_in_destination,
            "checksum_discrepancies": [
                {
                    "source_checksum": {
                        "relative_path": discrepancy.source_checksum.relative_path,
                        "checksum": discrepancy.source_checksum.checksum
                    },
                    "destination_checksum": {
                        "relative_path": discrepancy.destination_checksum.relative_path,
                        "checksum": discrepancy.destination_checksum.checksum
                    }
                } for discrepancy in comparison_result.checksum_discrepancies
            ]
        }
        dump(report_data, file, indent=4)
        print()
        print(f"Diff details report written to '{filename}'")
        print()


def print_summary(config: Configuration, comparison_result: ComparisonResult) -> None:
    print()
    print(f"Source path:                  {config.source_path}")
    print(f"Destination path:             {config.destination_path}")
    print(f"Workers per collector:        {config.worker_count_per_collector}")
    formatted_duration = format_duration(comparison_result.duration_millis)
    print(f"Elapsed time:                 {comparison_result.duration_millis} ms ({formatted_duration})")
    print(f"Files checked in source:      {comparison_result.number_of_files_in_source}")
    print(f"Files checked in destination: {comparison_result.number_of_files_in_destination}")
    print(f"Files missing in source:      {comparison_result.number_of_files_missing_in_source}")
    print(f"Files missing in destination: {comparison_result.number_of_files_missing_in_destination}")
    print(f"Checksum discrepancies:       {comparison_result.number_of_checksum_discrepancies}")
    print()


def main() -> None:
    try:
        config = parse_cmd_line_args()
        print_prestart_info(config)
        comparator = Comparator(config)
        comparison_result = comparator.compare()
        print_summary(config, comparison_result)
        write_json_report(comparison_result, config.diff_details_report)
    except Exception:
        print("ERROR!!! An unexpected exception has occurred.")
        print_exc()
        exit(1)


if __name__ == "__main__":
    main()
