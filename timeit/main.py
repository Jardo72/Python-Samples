from argparse import ArgumentParser, Namespace, RawTextHelpFormatter
from datetime import datetime
from timeit import timeit


def create_command_line_arguments_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Python timeit demo", formatter_class=RawTextHelpFormatter)

    parser.add_argument(
        "index",
        help="the index of the desired element of the Fibonacci sequence (0 correspondes to the 1st element, i.e. value 0)",
        type=int
    )
    parser.add_argument(
        "repetitions",
        help="desired number of repetitions (invocations) of the tested functions",
        type=int
    )

    return parser


def parse_command_line_arguments() -> Namespace:
    parser = create_command_line_arguments_parser()
    params = parser.parse_args()
    return params


def recursive_fibonacci(n: int) -> int:
    if n in {0, 1, 2}:
        return n
    
    return recursive_fibonacci(n - 2) + recursive_fibonacci(n - 1)


def plain_fibonacci(n: int) -> int:
    if n in {0, 1, 2}:
        return n

    a = 1
    b = 2
    for _ in range(3, n + 1):
        a, b = b, a + b
    return b


def current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    command_line_arguments = parse_command_line_arguments()
    repetitions = command_line_arguments.repetitions
    n = command_line_arguments.index
    print(f"Starting at {current_timestamp()}, ({repetitions} repetitions, n = {n})")
    plain_duration = timeit(stmt=lambda: plain_fibonacci(n), number=repetitions)
    recursive_duration = timeit(stmt=lambda: recursive_fibonacci(n), number=repetitions)
    print(f"Completed at {current_timestamp()}")
    print(f"n = {n}, repetitions = {repetitions}, plain duration = {plain_duration:.5f} sec, recursive duration = {recursive_duration:.5f} sec")



if __name__ == "__main__":
    main()
