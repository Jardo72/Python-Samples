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
from collections import deque
from itertools import product
from re import match
from typing import Tuple


def epilog() -> str:
    return """
This script finds all paths of a chess king from the given start square to the given
destination square of a chessboard. A path is valid if it does not exceed the specified
maximum number of moves. The squares are specified in numeric lowercase notation (e.g.
c1, d4, h8). The script checks for duplicate paths and raises an error if any are found.
"""


def create_cmd_line_args() -> ArgumentParser:
    parser = ArgumentParser(description="Chess King Path Finder", formatter_class=RawTextHelpFormatter, epilog=epilog())
    
    parser.add_argument(
        "start_square",
        help="the start square from which the path is to be found, in numeric lowercase notation (e.g. c1)"
    )
    parser.add_argument(
        "destination_square",
        help="the destination square to which the path is to be found, in numeric lowercase notation (e.g. c8)"
    )
    parser.add_argument(
        "max_moves",
        type=int,
        help="max. acceptable number of moves from the start square to the destination square"
    )

    return parser


def is_valid_square(square: str) -> bool:
    return match("^[a-h][1-8]$", square) is not None


def is_invalid_square(square: str) -> bool:
    return match("^[a-h][1-8]$", square) is None


def parse_cmd_line_args() -> Namespace:
    parser = create_cmd_line_args()
    result = parser.parse_args()
    if is_invalid_square(result.start_square):
        raise ValueError(f"Invalid start square: {result.start_square}")
    if is_invalid_square(result.destination_square):
        raise ValueError(f"Invalid destination square: {result.destination_square}")
    return result


def get_adjecant_squares(square: str) -> Tuple[str, ...]:
    result = []
    file = square[0]
    rank = int(square[1])

    ranks = [rank - 1, rank, rank + 1]
    files = [chr(ord(file) - 1), file, chr(ord(file) + 1)]
    result = list(map(lambda t: f"{t[0]}{t[1]}", product(files, ranks)))
    return tuple([s for s in result if is_valid_square(s) and s != square])


def search_paths(start_square: str, destination_square: str, max_moves: int) -> Tuple[Tuple[str, ...], ...]:
    queue = deque()
    result = []

    adjecant_squares = get_adjecant_squares(start_square)
    for square in adjecant_squares:
        path = [start_square, square]
        queue.append(path)
    while len(queue) > 0:
        path = queue.popleft()
        if path[-1] == destination_square:
            if len(path) - 1 <= max_moves:
                result.append(tuple(path))
            continue
        adjecant_squares = get_adjecant_squares(path[-1])
        for square in adjecant_squares:
            if square == destination_square:
                new_path = list(path)
                new_path.append(square)
                if len(new_path) - 1 <= max_moves:
                    result.append(tuple(new_path))
                continue
            if square not in path:
                new_path = list(path)
                new_path.append(square)
                if len(new_path) - 1 <= max_moves:
                    queue.append(new_path)

    return tuple(result)


def check_duplicates(paths: Tuple[Tuple[str, ...], ...]) -> None:
    paths_as_set = set(paths)
    assert len(paths) == len(paths_as_set)


# The Reti Idea
# https://www.youtube.com/watch?v=6WktW0S92es
def main() -> None:
    try:
        cmd_line_args = parse_cmd_line_args()
        print(f"Going search path(s) from {cmd_line_args.start_square} to {cmd_line_args.destination_square}")
        found_paths = search_paths(cmd_line_args.start_square, cmd_line_args.destination_square, cmd_line_args.max_moves)
        check_duplicates(found_paths)
        print()
        print(f"Totally {len(found_paths)} path(s) found from {cmd_line_args.start_square} to {cmd_line_args.destination_square}:")
        for path in found_paths:
            moves = len(path) -1
            print(f"{', '.join(path)} ({moves} {'move' if moves == 1 else 'moves'})")
    except ValueError as e:
        print("ERROR!!!")
        print(str(e))


if __name__ == "__main__":
    main()
