from time import perf_counter, sleep

from queue_automator.maybe import MultiprocessMaybe

import logging

logging.basicConfig(level=logging.DEBUG)


def do_work(item: int) -> int:
    sleep(2)
    result = item*2
    print(f'{item} times two {result}')
    return result


def do_work_2(item: int) -> int:
    sleep(2)
    result = item**2
    print(f'{item} squared {result}')
    return result


def do_work_3(item: int) -> int:
    sleep(2)
    result = item**3
    print(f'{item} cubed {result}')
    return result


if __name__ == '__main__':
    start = perf_counter()
    result = MultiprocessMaybe() \
        .insert(range(10)) \
        .then(do_work) \
        .insert(range(10, 20)) \
        .then(do_work_2) \
        .insert(range(20, 30)) \
        .maybe(do_work_3, default=0)

    end = perf_counter()

    print(result)
    print(f'Took {end-start:0.2f}s')
