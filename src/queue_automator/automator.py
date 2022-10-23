import logging
from multiprocessing import JoinableQueue, Manager, Process, Queue
from multiprocessing.managers import SyncManager
from typing import Callable, Dict, Iterable, List, Sequence, Union

from .constants import QueueFlags, QueueNames

logger = logging.getLogger('QueueAutomator')


class QueueAutomator:
    """
    QueueAutomator is a wrapper that provides an easy to use API to build
    queue multiprocessing pipelines

    Example:

    >>> automator = QueueAutomator()
    >>>
    >>> @automator.register_as_worker_function(output_queue_name='queue', process_count=2)
    >>> def do_work(item):
    >>>     ...
    >>>
    >>> @automator.register_as_worker_function(input_queue_name='queue', process_count=2)
    >>> def do_work_2(item):
    >>>     ...
    >>>
    >>> if __name__ == '__main__':
    >>>     automator.set_input_data([...]])
    >>>     results = automator.run()

    """

    def __init__(self, name: Union[str, None] = None) -> None:
        self.__queue_table: Dict[str, dict] = {
            QueueNames.OUTPUT: {
                'target': None,
                'process_count': None,
                'worker_function': None,
                'data': None
            },
        }
        self.name = name or ''

    def __repr__(self) -> str:
        return f'QueueAutomator[{self.name}]'

    def __validate_non_empty_args(self, args: Sequence) -> None:
        for arg in args:
            if not arg:
                raise ValueError(f'{arg} should not be empty or zero')

    def __build_queue(self, name: str, target: str, process_count: int, worker_function: Callable) -> dict:
        return {
            name: {
                'target': target,
                'process_count': process_count,
                'worker_function': worker_function,
                'data': None
            }
        }

    def __generate_queues(self, queues: list, manager: SyncManager, name: str) -> None:
        if name == QueueNames.OUTPUT:
            self.__queue_table[name]['queue'] = manager.Queue(0)
            return

        if name not in self.__queue_table:
            raise RuntimeError(f'{name} does not exist in queue map, register a worker function with input_queue_name={name}')

        current_queue = self.__queue_table[name]

        if current_queue.get('queue'):
            raise RuntimeError(f'{name} was already created, you may be creating a circular pipeline')

        next_queue = current_queue['target']
        current_queue['queue'] = manager.JoinableQueue()  # type: ignore
        queues.append((name, next_queue))

        return self.__generate_queues(queues, manager, next_queue)

    def __enqueue_data(self) -> None:

        for queue_name, queue_data in self.__queue_table.items():

            queue = queue_data.get('queue')
            if not queue:
                RuntimeError('enqueue_data was called for a non existent queue, this should not happen')

            data = queue_data.get('data', [])
            if not data:
                if queue_name == QueueNames.INPUT:
                    RuntimeError('data for input queue is empty, nothing to process')
                continue

            logger.debug(f'Inserting {len(data)} items in queue {queue_name}')
            for item in data:
                queue.put(item)  # type: ignore

    def _process_enqueued_objects(self, in_queue: JoinableQueue, out_queue: Queue, worker_function: Callable) -> None:

        while True:
            input_object = in_queue.get()
            if input_object != QueueFlags.EXIT:
                result = worker_function(input_object)
                out_queue.put(result)
                in_queue.task_done()
            else:
                in_queue.task_done()
                logger.debug('_>>> Done <<<_')
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
            logger.debug(f'Started {process.name} for queue {in_queue_name}')

        return process_list

    def __join_processes(self, process_list: list) -> None:
        for process in process_list:
            process.join()

    def __signal_queue_exit(self, queue: JoinableQueue, num_processes: int) -> None:
        for _ in range(num_processes):
            queue.put(QueueFlags.EXIT)

    def __recover_from_queue(self, queue: Queue, manager: bool = False) -> list:
        results = []
        while not queue.empty():
            results.append(queue.get())
            if manager:
                queue.task_done()
        return results

    def set_data_for_queue(self, data: Iterable, queue: str) -> None:

        logger.debug(f'Setting data for queue {queue}')

        if queue == QueueNames.OUTPUT:
            raise RuntimeError('trying to set data directly to the output queue')

        if queue not in self.__queue_table:
            raise RuntimeError('trying to set data to an unexistent queue')

        self.__queue_table[queue]['data'] = data

    def set_input_data(self, input_data: Iterable) -> None:
        """
        This function is used to set the data to be processed at the start of the pipeline

        Args:
            input_data (Iterable)
        """
        self.set_data_for_queue(input_data, QueueNames.INPUT)

    def register_as_worker_function(self, input_queue_name: str = QueueNames.INPUT,
                                    output_queue_name: str = QueueNames.OUTPUT,
                                    process_count: int = 1) -> Callable:
        """
        Decorator to register your functions to process data as part of a multiprocessing queue pipeline

        Args:
            input_queue_name (str, optional): The name of the input queue for this function. Defaults to 'input'.
            output_queue_name (Union[str, None], optional): the name of the output queue for this function. Defaults to None.
            process_count (int, optional): The ammount of processes to listen to the given input queue. Defaults to 1.

        Raises:
            RuntimeError: If input_queue_name is already registered, use unique names
            ValueError: If input_queue_name is none or process_count is <= 0

        Returns:
            Callable: The wrapped function after registering it.
        """

        self.__validate_non_empty_args((input_queue_name, process_count, output_queue_name))

        if input_queue_name in self.__queue_table:
            raise RuntimeError(f'{input_queue_name} already exists in queue table, pick another name')

        if process_count < 0:
            raise ValueError('process_count cannot be a negative number')

        def store_in_queue_table_wrapper(func: Callable) -> Callable:
            self.__queue_table.update(
                self.__build_queue(input_queue_name, output_queue_name or QueueNames.OUTPUT, process_count, func)
            )
            return func

        return store_in_queue_table_wrapper

    def run(self) -> list:
        """
        Is the main entry point to execute your program
        with a multiprocessing queue pipeline.

        To use it you need to register at least 1 worker function

        Do not forget to call set_input_data(Iterable) before calling run()

        Returns:
            list: The output as a simple python list
        """

        manager = Manager()
        queues: List[tuple] = []

        self.__generate_queues(queues, manager, QueueNames.INPUT)

        process_per_queue = tuple((input_queue, self.__spawn_processes(input_queue, output_queue)) for input_queue, output_queue in queues)

        self.__enqueue_data()

        for queue_name, procesess in process_per_queue:
            current_queue = self.__queue_table[queue_name]
            current_queue['queue'].join()
            self.__signal_queue_exit(current_queue['queue'], current_queue['process_count'])
            self.__join_processes(procesess)

        return self.__recover_from_queue(self.__queue_table[QueueNames.OUTPUT]['queue'], True)

    def reset(self) -> None:
        self.__queue_table = {
            QueueNames.OUTPUT: {
                'target': None,
                'process_count': None,
                'worker_function': None,
                'data': None
            }
        }
