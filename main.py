from multiprocessing import JoinableQueue, Manager, Process, Queue
from multiprocessing.managers import SyncManager
from time import sleep, perf_counter
from typing import Callable, Iterable, List, Union


class QueueNames:
    INPUT: str = 'input'
    OUTPUT: str = 'output'


class QueueFlags:
    EXIT: str = 'EXIT_QUEUE'


class QueueAutomator:

    def __init__(self) -> 'QueueAutomator':
        self.__queue_table: dict = {
            QueueNames.OUTPUT: {
                'target': None,
                'process_count': None,
                'worker_function': None
            },
        }
        self.input_data = None

    def set_input_data(self, input_data: Iterable):
        self.input_data = input_data

    def __validate_non_empty_args(self, args: list):
        for arg in args:
            if not args:
                raise ValueError(f'{arg} should not be empty or zero')

    def __build_queue(self, name: str, target: str, process_count: int, worker_function: Callable) -> dict:
        return {
            name: {
                'target': target,
                'process_count': process_count,
                'worker_function': worker_function
            }
        }

    def register_as_worker_function(self, input_queue_name: str = QueueNames.INPUT, output_queue_name: Union[str, None] = None,  process_count: int = 1) -> Callable:
        self.__validate_non_empty_args((input_queue_name, process_count))

        if input_queue_name in self.__queue_table:
            raise RuntimeError(f'{input_queue_name} already exists in queue map, pick another name')

        def store_in_queue_table(func: Callable) -> Callable:
            self.__queue_table.update(
                self.__build_queue(input_queue_name, output_queue_name or QueueNames.OUTPUT, process_count, func)
            )
            return func

        return store_in_queue_table

    def __generate_queues(self, queues: list, manager: SyncManager, name: str):
        if name not in self.__queue_table:
            raise RuntimeError(f'{name} does not exist in queue map, register an input generator function')

        if name == QueueNames.OUTPUT:
            self.__queue_table[name]['queue'] = manager.Queue(0)
            return

        current_queue = self.__queue_table[name]
        next_queue = current_queue['target']
        current_queue['queue'] = manager.JoinableQueue()
        queues.append((name, next_queue))

        return self.__generate_queues(queues, manager, next_queue)

    def __enqueue_input_data(self):
        input_queue = self.__queue_table[QueueNames.INPUT].get('queue')
        if not input_queue:
            RuntimeError('enqueue_items was called before input queue was initialized, this should not happen')

        if not self.input_data:
            RuntimeError('input_data is empty, no data to process')

        for item in self.input_data:
            input_queue.put(item)

    def _process_enqueued_objects(self, in_queue: JoinableQueue, out_queue: Queue, worker_function: Callable):

        while True:
            input_object = in_queue.get()
            if input_object != QueueFlags.EXIT:
                result = worker_function(input_object)
                out_queue.put(result)
                in_queue.task_done()
            else:
                in_queue.task_done()
                print('_>>> Done <<<_')
                return

    def __spawn_processes(self, in_queue_name: str, out_queue_name: str) -> List[Process]:
        in_queue = self.__queue_table[in_queue_name]
        out_queue = self.__queue_table[out_queue_name]
        target = self._process_enqueued_objects

        process_list = list()
        for _ in range(in_queue['process_count']):
            process = Process(target=target, args=(in_queue['queue'], out_queue['queue'], in_queue['worker_function']))
            process.start()
            process_list.append(process)
            print(f'started process => {process.name}')

        return process_list

    def __join_processes(self, process_list: list):
        for process in process_list:
            process.join()

    def __signal_queue_exit(self, queue: JoinableQueue, num_processes: int):
        for _ in range(num_processes):
            queue.put(QueueFlags.EXIT)

    def __recover_from_queue(self, queue: Queue, manager=False) -> list:
        results = []
        while not queue.empty():
            results.append(queue.get())
            if manager:
                queue.task_done()
        return results

    def run(self) -> list:

        manager = Manager()
        queues = []
        process_per_queue = {}

        self.__generate_queues(queues, manager, QueueNames.INPUT)

        for input_queue, output_queue in queues:
            process_per_queue.update({input_queue: self.__spawn_processes(input_queue, output_queue)})

        self.__enqueue_input_data()

        for queue_name, procesess in process_per_queue.items():
            current_queue = self.__queue_table[queue_name]
            current_queue['queue'].join()
            self.__signal_queue_exit(current_queue['queue'], current_queue['process_count'])
            self.__join_processes(procesess)

        results = self.__recover_from_queue(self.__queue_table[QueueNames.OUTPUT]['queue'], True)
        return results


automator = QueueAutomator()


@automator.register_as_worker_function(output_queue_name='square_queue', process_count=2)
def do_work(item):
    sleep(1)
    result = item*2
    print(f'{item} times two {result}')
    return result


@automator.register_as_worker_function(input_queue_name='square_queue', process_count=2)
def do_work_2(item):
    sleep(1)
    result = item**2
    print(f'{item} squared {result}')
    return result


def run_sync(input_data):
    results = []
    for item in input_data:
        res = do_work(item)
        res = do_work_2(res)
        results.append(res)
    return results


if __name__ == '__main__':
    start = perf_counter()
    input_data = range(30)
    automator.set_input_data(input_data)
    results = automator.run()
    # results = run_sync(input_data)
    print(results)
    end = perf_counter()

    print(f'took {end-start:0.2f}s')
