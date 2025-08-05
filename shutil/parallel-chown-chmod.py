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
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from os import chmod, cpu_count, scandir
from pathlib import Path
from re import match
from shutil import chown
from traceback import print_exc

from commons import format_duration, Sequence, Stopwatch


MAX_WORKERS = 16


@dataclass(frozen=True)
class Configuration:
    root_path: str
    user: str
    group: str
    permissions: int
    workers: int
    dry_run: bool


@dataclass(frozen=True)
class Request:
    seq_no: int
    path: str
    files: tuple[str, ...]
    user: str
    group: str
    permissions: int


@dataclass(frozen=True)
class Result:
    request: Request
    modified_file_count: int
    failed_file_count: int
    modified_dir_count: int
    failed_dir_count: int


@dataclass(frozen=True)
class Summary:
    overall_duration_millis: int
    modified_file_count: int
    failed_file_count: int
    modified_dir_count: int
    failed_dir_count: int
    exception_count: int


class Processor:
    def __init__(self, config: Configuration):
        self._config = config
        self._executor = ProcessPoolExecutor(max_workers=config.workers)
        self._sequence = Sequence()
        self._future_list = []

    def _scan_directory(self, path: str) -> None:
        directories = []
        files = []
        with scandir(path) as entries:
            for entry in entries:
                if entry.is_dir():
                    directories.append(entry.path)
                elif entry.is_file():
                    files.append(entry.path)
            request = Request(
                seq_no=self._sequence.next_value(),
                path=path,
                files=tuple(files),
                user=self._config.user,
                group=self._config.group,
                permissions=self._config.permissions,
            )
            future = self._executor.submit(process_request, request, self._config.dry_run)
            self._future_list.append(future)
        for dir in directories:
            self._scan_directory(dir)

    def _collect_results(self) -> Summary:
        exception_count = 0
        modified_file_count = 0
        failed_file_count = 0
        modified_dir_count = 0
        failed_dir_count = 0
        for future in self._future_list:
            try:
                result = future.result()
                modified_file_count += result.modified_file_count
                failed_file_count += result.failed_file_count
                modified_dir_count += result.modified_dir_count
                failed_dir_count += result.failed_dir_count
            except Exception as e:
                print(f"Error in processing: {e}")
                exception_count += 1

        return Summary(
            overall_duration_millis=self._stopwatch.elapsed_time_millis(),
            modified_file_count=modified_file_count,
            failed_file_count=failed_file_count,
            modified_dir_count=modified_dir_count,
            failed_dir_count=failed_dir_count,
            exception_count=exception_count,
        )

    def process(self) -> Summary:
        self._stopwatch = Stopwatch.start()
        self._scan_directory(self._config.root_path)
        return self._collect_results()


    def __enter__(self) -> Processor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._executor:
            self._executor.shutdown(wait=True)
        self._executor = None
    

def epilog() -> str:
    return """
This script modifies the ownership and the permissions of the files and directories within
the given directory structure. The modification is recursive and parallel. The specified
user and group are applied to all files and directories.
"""


def is_valid_user(user_name: str) -> bool:
    try:
        from pwd import getpwnam
        getpwnam(user_name)
        return True
    except KeyError:
        return False
    except ModuleNotFoundError:
        print("The 'pwd' module is not available on this platform. User validation will be skipped.")
        return True
    

def is_valid_group(group_name: str) -> bool:
    try:
        from grp import getgrnam
        getgrnam(group_name)
        return True
    except KeyError:
        return False
    except ModuleNotFoundError:
        print("The 'grp' module is not available on this platform. Group validation will be skipped.")
        return True
    

def create_cmd_line_args_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Parallel Modification of Ownership and Permissions for Files and Directories",
        formatter_class=RawTextHelpFormatter,
        epilog=epilog()
    )

    # positional mandatory arguments
    parser.add_argument("root_path",
        help = "path to the root directory of the directory tree to process",
        type=str)
    parser.add_argument("user",
        help = "the username of the new owner",
        type=str)
    parser.add_argument("group",
        help = "the group name of the new group owner",
        type=str)
    parser.add_argument("permissions",
        help = "the permissions to set (e.g. 640 = read/write for owner, read for group, no permissions for others)",
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
        help="if specified, the script will not change any ownership or permissions (it will only print what would be changed)"
    )

    return parser


def parse_cmd_line_args() -> Configuration:
    parser = create_cmd_line_args_parser()
    params = parser.parse_args()
    if not Path(params.root_path).is_dir():
        parser.error(f"Root path '{params.root_path}' is not a valid directory.")
    if not is_valid_user(params.user):
        parser.error(f"User with the username '{params.user}' does not exist.")
    if not is_valid_group(params.group):
        parser.error(f"Group with the name '{params.group}' does not exist.")
    if not 1 <= params.workers <= MAX_WORKERS:
        parser.error(f"Number of workers must be a positive integer between 1 and {MAX_WORKERS}")
    if not match(r'^[0-7]{3}$', params.permissions):
        parser.error(f"Permissions '{params.permissions}' must be a 3 digit number (e.g. 640).")
    return Configuration(
        root_path=params.root_path,
        user=params.user,
        group=params.group,
        permissions=int(params.permissions),
        workers=params.workers,
        dry_run=params.dry_run,
    )


def print_prestart_info(config: Configuration) -> None:
    current_timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z (%z)")
    print()
    print(f"Going to change ownership & permissions for '{config.root_path}'")
    print(f"{cpu_count()} CPU core(s) detected, {config.workers} worker process(es) will be used")
    print(f"Start time {current_timestamp}")
    print()


def process_request(request: Request, dry_run: bool) -> Result:
    print(f"Going to process directory '{request.path}' (request #{request.seq_no})")
    modified_file_count = 0
    failed_file_count = 0
    modified_dir_count = 0
    failed_dir_count = 0

    try:
        if not dry_run:
            chown(request.path, user=request.user, group=request.group)
            chmod(request.path, request.permissions)
        modified_dir_count += 1
    except Exception as e:
        print(f"Error processing directory {request.path}: {e}")
        failed_dir_count += 1

    for file_name in request.files:
        file_path = Path(request.path) / file_name
        try:
            if not dry_run:
                chown(file_path, user=request.user, group=request.group)
                chmod(file_path, request.permissions)
            modified_file_count += 1
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            failed_file_count += 1

    return Result(
        request=request,
        modified_file_count=modified_file_count,
        failed_file_count=failed_file_count,
        modified_dir_count=modified_dir_count,
        failed_dir_count=failed_dir_count,
    )


def apply_ownership_and_permissions(config: Configuration) -> Summary:
    with Processor(config) as processor:
        return processor.process()


def print_summary(config: Configuration, summary: Summary) -> None:
    formatted_duration = format_duration(summary.overall_duration_millis)
    print()
    print(f"Root path:            {config.root_path}")
    print(f"Workers used:         {config.workers}")
    print(f"Overall duration:     {summary.overall_duration_millis} ms ({formatted_duration})")
    print(f"Modified files:       {summary.modified_file_count}")
    print(f"Failed files:         {summary.failed_file_count}")
    print(f"Modified directories: {summary.modified_dir_count}")
    print(f"Failed directories:   {summary.failed_dir_count}")
    print(f"Exceptions:           {summary.exception_count}")
    print(f"Dry run:              {'yes' if config.dry_run else 'no'}")
    print()


def main() -> None:
    try:
        config = parse_cmd_line_args()
        print_prestart_info(config)
        summary = apply_ownership_and_permissions(config)
        print_summary(config, summary)
    except Exception as e:
        print("ERROR!!! An unexpected exception has occurred.")
        print_exc()
        exit(1)


if __name__ == "__main__":
    main()
