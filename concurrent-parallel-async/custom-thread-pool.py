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

from collections import deque
from colorama import init, Fore
from random import randint
from threading import Condition, Lock, Thread
from time import sleep
from typing import Callable


class BlockingQueue:

    def __init__(self) -> None:
        self._lock = Lock()
        self._condition = Condition(lock=self._lock)
        self._queue = deque()

    def enqueue(self, action: Callable) -> int:
        with self._lock:
            self._queue.append(action)
            current_size = len(self._queue)
            if current_size == 1:
                self._condition.notify_all()
            return current_size

    def dequeue(self) -> Callable:
        with self._lock:
            while len(self._queue) == 0:
                self._condition.wait()
            return self._queue.popleft()


class ThreadPool:
    
    def __init__(self, min_workers: int, max_workers: int) -> None:
        self._queue = BlockingQueue()
        self._lock = Lock()
        self._min_workers = min_workers
        self._max_workers = max_workers
        self._workers = [WorkerThread(self._queue) for _ in range(0, self._min_workers)]

    def submit(self, action: Callable) -> None:
        queue_size = self._queue.enqueue(action)
        with self._lock:
            if queue_size > 5 and len(self._workers) < self._max_workers:
                self._workers.append(WorkerThread(self._queue))
                print(f"{Fore.GREEN} New worker started, current number of workers is {len(self._workers)}")


class Sequence:

    def __init__(self) -> None:
        self._current_value = 0
        self._lock = Lock()

    def next_value(self) -> int:
        with self._lock:
            self._current_value += 1
            return self._current_value
        

class WorkerThread(Thread):
    
    _sequence = Sequence()

    _color_mapping = {
        "Worker-1": Fore.BLUE,
        "Worker-2": Fore.CYAN,
        "Worker-3": Fore.YELLOW,
        "Worker-4": Fore.RED,
        "Worker-5": Fore.MAGENTA,
        "Worker-6": Fore.LIGHTRED_EX,
        "Worker-7": Fore.LIGHTGREEN_EX,
        "Worker-8": Fore.LIGHTBLUE_EX,
        "Worker-9": Fore.LIGHTMAGENTA_EX,
        "Worker-10": Fore.LIGHTYELLOW_EX,
    }

    def __init__(self, queue: BlockingQueue) -> None:
        super().__init__(name=f"Worker-{self._sequence.next_value()}")
        self._queue = queue
        self.start()

    def run(self) -> None:
        while True:
            action = self._queue.dequeue()
            fore = self._color_mapping[self.name]
            print(f"{fore} {self.name} is going to execute task")
            action()
            print(f"{fore} {self.name} has completed task execution")


def main() -> None:
    init()
    thread_pool = ThreadPool(3, 10)
    for _ in range(0, 20):
        thread_pool.submit(lambda: sleep(5 + randint(0, 10)))


if __name__ == "__main__":
    main()
