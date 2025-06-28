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

from asyncio import create_task, run, sleep, CancelledError
from itertools import cycle
from random import random, randint


async def show_progress() -> None:
    for char in cycle("\|/-"):
        status = f"\r{char} thinking" 
        print(status, end="", flush=True)
        try:
            await sleep(0.25)
        except CancelledError: 
            print("\r" * len(status), end="", flush=True)
            return


async def think() -> int:
    await sleep(1 + 7 * random())
    return randint(1, 100)


async def async_main() -> int:
    coroutine = create_task(show_progress())
    result = await think()
    coroutine.cancel()
    return result


def main() -> None:
    result = run(async_main())
    print(f"Outcome of thinking = {result}")


if __name__ == "__main__":
    main()
