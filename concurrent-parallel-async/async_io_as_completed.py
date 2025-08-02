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

from asyncio import as_completed, run, sleep
from random import random


async def get_message(i: int) -> str:
    duration = 10 * random()
    await sleep(duration)
    return f"Message #{i} (sleep = {duration:.2f} sec)"


async def run_test() -> tuple[str, ...]:
    couroutines = [get_message(i) for i in range(1, 21)]
    result = []
    for future in as_completed(couroutines):
        message = await future
        result.append(message)
    return tuple(result)


def main() -> None:
    messages = run(run_test())
    for message in messages:
        print(message)


if __name__ == "__main__":
    main()
