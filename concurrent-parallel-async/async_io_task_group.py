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

from asyncio import TaskGroup, run, sleep
from itertools import cycle
from random import randint

from colorama import init, Fore


async def factorial(n: int, color: str) -> str:
    if n < 0:
        raise ValueError("Factorial is not defined for negative numbers.")
    result = 1
    for i in range(2, n + 1):
        result *= i
        if i % 50 == 0:
            print(f"{color}Task yielded control...")
            await sleep(0)
    print(f"{color}Task ready...")
    return f"factorial({n}) = {result}"


async def run_tasks() -> tuple[str, ...]:
    colors = cycle([Fore.RED, Fore.GREEN, Fore.BLUE, Fore.MAGENTA, Fore.CYAN, Fore.YELLOW])
    async with TaskGroup() as task_group:
        task_list = []
        for _ in range(5):
            task = task_group.create_task(factorial(randint(100, 200), next(colors)))
            task_list.append(task)

        result_list = []
        for task in task_list:
            try:
                result = await task
                result_list.append(result)
            except Exception as e:
                print(f"Task failed with exception: {e}")

        return tuple(result_list)


def main() -> None:
    init(autoreset=True)
    result_list = run(run_tasks())
    print()
    print("Results of all tasks:")
    for result in result_list:
        print(result)


if __name__ == "__main__":
    main()
