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
