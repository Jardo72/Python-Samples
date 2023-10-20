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
