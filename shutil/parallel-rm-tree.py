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
from os import listdir, remove
from os.path import isdir, join
from pathlib import Path
from shutil import rmtree

from commons import format_duration, Sequence, Stopwatch


MAX_WORKERS = 16


@dataclass(frozen=True)
class Configuration:
    root_path: str
    workers: int
    dry_run: bool


@dataclass(frozen=True)
class Request:
    seq_no: int
    path: str
    dry_run: bool


@dataclass(frozen=True)
class Summary:
    duration_millis: int
    removed_tree_count: int
    failure_count: int


def epilog() -> str:
    return """
This script removes a given directory structure in parallel using multiple threads.
It is designed to handle large directory trees efficiently. The root directory of the
tree is not removed - only its subdirectories and files are deleted recursively.
"""


def create_cmd_line_args_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Parallel Removal of Directory Structures",
        formatter_class=RawTextHelpFormatter,
        epilog=epilog()
    )

    # positional mandatory arguments
    parser.add_argument("root_path",
        help = "path to the root directory of the directory tree to be deleted",
        type=str)

    # optional arguments
    parser.add_argument("-w", "--workers",
        dest="workers",
        default=4,
        help=f"optional number of worker threads to be used (default = 4, max = {MAX_WORKERS})",
        type=int)
    parser.add_argument("-d", "--dry-run",
        dest="dry_run",
        default=False,
        action="store_true",
        help="if specified, the script will not delete any directories or files (it will only print what would be deleted)"
    )

    return parser


def parse_cmd_line_args() -> Configuration:
    parser = create_cmd_line_args_parser()
    params = parser.parse_args()
    if not Path(params.root_path).is_dir():
        parser.error(f"Root path '{params.root_path}' is not a valid directory.")
    if not 1 <= params.workers <= MAX_WORKERS:
        parser.error(f"Number of workers must be a positive integer between 1 and {MAX_WORKERS}")
    return Configuration(
        root_path=params.root_path,
        workers=params.workers,
        dry_run=params.dry_run,
    )


def remove_directory(request: Request) -> None:
    if request.dry_run:
        print(f"Would remove directory: '{request.path}' (request #{request.seq_no})")
    else:
        print(f"Removing directory: '{request.path}' (request #{request.seq_no})")
        rmtree(request.path, ignore_errors=False)


def remove_subdir_trees(config: Configuration) -> Summary:
    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        stopwatch = Stopwatch.start()
        future_list = []
        sequence = Sequence()
        for entry in listdir(config.root_path):
            entry_path = join(config.root_path, entry)
            if isdir(entry_path):
                request = Request(
                    seq_no=sequence.next_value(),
                    path=entry_path,
                    dry_run=config.dry_run,
                )
                future = executor.submit(remove_directory, request)
                future_list.append(future)
            else:
                remove(entry_path)

        removed_tree_count = 0
        failure_count = 0
        for future in future_list:
            try:
                future.result()
                removed_tree_count += 1
            except Exception as e:
                print(f"Error removing directory: {e}")
                failure_count += 1
        duration_millis = stopwatch.elapsed_time_millis()

        return Summary(
            duration_millis=duration_millis,
            removed_tree_count=removed_tree_count,
            failure_count=failure_count,
        )


def print_summary(summary: Summary) -> None:
    print()
    formatted_duration = format_duration(summary.duration_millis)
    print("Summary:")
    print(f"Removed {summary.removed_tree_count} directory trees in {summary.duration_millis} ms ({formatted_duration})")
    if summary.failure_count > 0:
        print(f"Failed to remove {summary.failure_count} directory trees")
    else:
        print("All directory trees removed successfully")
    print()


def main() -> None:
    config = parse_cmd_line_args()
    summary = remove_subdir_trees(config)
    print_summary(summary)


if __name__ == "__main__":
    main()
