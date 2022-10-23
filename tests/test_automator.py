from multiprocessing import Manager, Queue
from typing import Any

import pytest
from src.queue_automator import QueueAutomator
from src.queue_automator.constants import QueueFlags, QueueNames


@pytest.mark.parametrize('name', [None, 'TestName'])
def test_constructor(name: str) -> None:
    automator = QueueAutomator(name)

    assert automator._QueueAutomator__queue_table == {
        'output': {
            'target': None,
            'process_count': None,
            'worker_function': None,
            'data': None
        }
    }
    test_name = name or ''
    assert automator.name == test_name


def test_repr() -> None:
    automator = QueueAutomator('test')
    assert str(automator) == 'QueueAutomator[test]'


@pytest.mark.parametrize('args,result', [((None, 'a', 'b'), ValueError), (('a', 'b', 'c'), None)])
def test_validate_non_empty_args(args: tuple, result: Any) -> None:
    automator = QueueAutomator('test')
    if result:
        with pytest.raises(ValueError):
            automator._QueueAutomator__validate_non_empty_args(args)
    else:
        automator._QueueAutomator__validate_non_empty_args(args)


def test_build_queue() -> None:
    name = 'name'
    target = 'target'
    process_count = 0
    def worker_function(x: Any) -> Any: return x

    automator = QueueAutomator()
    assert automator._QueueAutomator__build_queue(name, target, process_count, worker_function) == {
        name: {
            'target': target,
            'process_count': process_count,
            'worker_function': worker_function,
            'data': None
        }
    }


def test_generate_output_queue() -> None:
    automator = QueueAutomator()

    queues: list = []
    automator._QueueAutomator__generate_queues(queues, Manager(), QueueNames.OUTPUT)

    generated_queue = automator._QueueAutomator__queue_table[QueueNames.OUTPUT]['queue']
    assert len(queues) == 0
    assert generated_queue
    assert 'Queue' in str(type(generated_queue))


def test_generate_invalid_queue() -> None:

    automator = QueueAutomator()
    queues: list = []

    with pytest.raises(RuntimeError):
        automator._QueueAutomator__generate_queues(queues, Manager(), 'invalid_name')

    assert len(queues) == 0


def test_generate_queues() -> None:
    automator = QueueAutomator()

    automator.register_as_worker_function()(lambda x: x)

    queues: list = []

    automator._QueueAutomator__generate_queues(queues, Manager(), 'input')
    generated_queue = automator._QueueAutomator__queue_table['input']['queue']

    name, next_queue_name = queues[0]

    assert len(queues) == 1
    assert name == 'input'
    assert next_queue_name == 'output'
    assert generated_queue


def test_generate_same_queue_twice_is_invalid() -> None:
    automator = QueueAutomator()

    automator.register_as_worker_function()(lambda x: x)

    queues: list = []
    manager = Manager()

    automator._QueueAutomator__generate_queues(queues, manager, 'input')
    with pytest.raises(RuntimeError):
        automator._QueueAutomator__generate_queues(queues, manager, 'input')


def test_process_enqueued_objects() -> None:

    manager = Manager()

    in_queue = manager.JoinableQueue()  # type: ignore
    out_queue = manager.Queue(0)
    automator = QueueAutomator()

    in_queue.put('value')
    in_queue.put(QueueFlags.EXIT)

    automator._process_enqueued_objects(in_queue, out_queue, lambda x: x)

    assert out_queue.get() == 'value'
    out_queue.task_done()


def mock_func(x: Any) -> Any: return x


def test_spawn_and_join_processes() -> None:
    automator = QueueAutomator()
    process_count = 2
    automator.register_as_worker_function(process_count=process_count)(mock_func)

    queues: list = []
    automator._QueueAutomator__generate_queues(queues, Manager(), QueueNames.INPUT)
    process_list = automator._QueueAutomator__spawn_processes(QueueNames.INPUT, QueueNames.OUTPUT)
    input_queue = automator._QueueAutomator__queue_table[QueueNames.INPUT]['queue']
    automator._QueueAutomator__signal_queue_exit(input_queue, process_count)

    assert len(process_list) == process_count
    assert len(queues) == 1

    automator._QueueAutomator__join_processes(process_list)


def test_reset() -> None:
    automator = QueueAutomator()
    automator.register_as_worker_function()(mock_func)
    assert len(automator._QueueAutomator__queue_table) == 2
    assert QueueNames.INPUT in automator._QueueAutomator__queue_table
    assert QueueNames.OUTPUT in automator._QueueAutomator__queue_table

    automator.reset()

    assert len(automator._QueueAutomator__queue_table) == 1
    assert QueueNames.INPUT not in automator._QueueAutomator__queue_table
    assert QueueNames.OUTPUT in automator._QueueAutomator__queue_table


@pytest.mark.parametrize('queue,has_manager', ((Manager().Queue(), True), (Manager().Queue(), False)))
def test_recover_from_queue(queue: Queue, has_manager: bool) -> None:
    automator = QueueAutomator()
    data = list(range(10))
    for item in data:
        queue.put(item)
    result = automator._QueueAutomator__recover_from_queue(queue, has_manager)
    assert isinstance(result, list)
    assert data == result
