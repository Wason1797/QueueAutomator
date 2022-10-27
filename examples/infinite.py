from time import perf_counter, sleep

from queue_automator import QueueAutomator

from threading import Thread
import logging

logging.basicConfig(level=logging.DEBUG)

automator = QueueAutomator()


@automator.register_as_worker_function(process_count=6)
def do_work(item: int) -> int:
    sleep(2)
    result = item*2
    print(f'{item} times two {result}')
    return result


def run_automator(automator: QueueAutomator) -> None:
    automator.set_input_data([])
    automator.run(forever=True)


def process_data(automator: QueueAutomator) -> None:
    end_at = 10
    current = 0
    while end_at > current:
        current += 1
        automator.set_data_for_queue(range(5), 'input', imediate=True)
        sleep(1)

    out = automator.get_output()
    print(f'finished with {len(out)} values in output')


if __name__ == '__main__':
    start = perf_counter()
    t1 = Thread(target=run_automator, args=(automator,))
    t2 = Thread(target=process_data, args=(automator,))

    t1.start()
    t1.join()

    t2.start()
    t2.join()

    end = perf_counter()

    print(f'Took {end-start:0.2f}s')
