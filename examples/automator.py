from time import perf_counter, sleep

from queue_automator import QueueAutomator

import logging

logging.basicConfig(level=logging.DEBUG)

automator = QueueAutomator()


@automator.register_as_worker_function(output_queue_name='square_queue', process_count=2)
def do_work(item: int) -> int:
    sleep(2)
    result = item*2
    print(f'{item} times two {result}')
    return result


@automator.register_as_worker_function(input_queue_name='square_queue', output_queue_name='cube_queue', process_count=2)
def do_work_2(item: int) -> int:
    sleep(2)
    result = item**2
    print(f'{item} squared {result}')
    return result


@automator.register_as_worker_function(input_queue_name='cube_queue', output_queue_name='add_2_queue', process_count=2)
def do_work_3(item: int) -> int:
    sleep(2)
    result = item**3
    print(f'{item} cubed {result}')
    return result


@automator.register_as_worker_function(input_queue_name='add_2_queue', process_count=2)
def do_work_4(item: int) -> int:
    sleep(2)
    result = item+2
    print(f'{item} + 2 {result}')
    return result


if __name__ == '__main__':
    start = perf_counter()
    input_data = range(30)
    automator.set_input_data(input_data)
    results = automator.run()
    end = perf_counter()

    print(results)
    print(f'Took {end-start:0.2f}s')
