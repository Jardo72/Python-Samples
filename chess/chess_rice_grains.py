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

class SquareGenerator:

    def __init__(self):
        self._current_rank = 1
        self._current_file = 'a'

    def next_square(self) -> str:
        result = self._current_file + str(self._current_rank)
        if self._current_file == 'h':
            self._current_file = 'a'
            self._current_rank += 1
        else:
            self._current_file = chr(ord(self._current_file) + 1)
        return result
        

square_generator = SquareGenerator()
grain_counts = []
for i in range(0, 64):
    square = square_generator.next_square()
    grains_on_square = 1 if i == 0 else 2 * grain_counts[i - 1]
    print(f"{square}: {grains_on_square}")
    grain_counts.append(grains_on_square)
print()
print(f"Sum: {sum(grain_counts)}")
