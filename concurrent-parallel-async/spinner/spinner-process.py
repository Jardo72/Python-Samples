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

from itertools import cycle
from random import random, randint
from time import sleep
from multiprocessing import Event, Process


def show_progress(event: Event) -> None:
    for char in cycle("\|/-"):
        status = f"\r{char} thinking" 
        print(status, end="", flush=True)
        if event.wait(0.25):
            print("\r" * len(status), end="", flush=True)
            return


def think() -> int:
    sleep(1 + 7 * random())
    return randint(1, 100)


def main() -> None:
    completion_event = Event()
    progress_indicator = Process(target=show_progress, args=(completion_event,))
    progress_indicator.start()
    result = think()
    completion_event.set()
    progress_indicator.join()
    print(f"Outcome of thinking = {result}")


if __name__ == "__main__":
    main()
