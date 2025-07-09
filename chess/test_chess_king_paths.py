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

import pytest

from chess_king_paths import search_paths


def test_there_is_no_path_to_the_same_square():
    paths = search_paths("e4", "e4", 0)

    # no path means empty tuple
    assert paths == ()


def test_single_move_path():
    paths = search_paths("e4", "d4", 1)

    # there is only one path from to an adjacent square if only one move is allowed
    assert len(paths) == 1
    assert ("e4", "d4") in paths


def test_two_moves_path():
    paths = search_paths("e4", "c4", 2)

    # all paths should start with e4 and end with c4, and have at most 2 moves
    assert all(p[0] == "e4" and p[-1] == "c4" for p in paths)
    assert all(len(p) - 1 <= 2 for p in paths)

    # there are three possible paths in two moves
    assert len(paths) == 3
    assert ("e4", "d5", "c4") in paths
    assert ("e4", "d4", "c4") in paths
    assert ("e4", "d3", "c4") in paths


def test_there_is_no_path_with_too_few_moves():
    paths = search_paths("e4", "c4", 1)

    # c4 cannot be reached from e4 in 1 move
    assert paths == ()


def test_corner_to_corner():
    paths = search_paths("a1", "h8", 7)

    # there is only one path from a1 to h8 in 7 moves - the main diagonal
    assert len(paths) == 1
    assert paths[0] == ("a1", "b2", "c3", "d4", "e5", "f6", "g7", "h8")


def test_all_paths_are_unique():
    paths = search_paths("d4", "e5", 2)

    assert len(paths) == len(set(paths))