from functools import lru_cache
from math import ceil
from os import cpu_count
from typing import Any, Callable, Iterable, List, Optional

from .automator import QueueAutomator
from .constants import QueueNames


class MaybeWrapper:

    def __init__(self, func: Callable, nothing_check: Callable, default: Any = None) -> None:
        self.func = func
        self.nothing_check = nothing_check
        self.default = default

    def maybe(self, value: Any) -> Any:
        return (value if self.default is None else self.default) if self.nothing_check(value) else self.func(value)


class MultiprocessMaybe:

    def __init__(self, nothing_check: Optional[Callable] = None) -> None:
        self.automator = QueueAutomator('MaybeAutomator')
        self.__call_stack: List[tuple] = []
        self.__inserted_data: dict[int, Iterable] = {}
        self.nothing_check = nothing_check

    def _is_nothing(self, value: Any) -> bool:
        return self.nothing_check(value) if self.nothing_check else not value

    @lru_cache
    def __balance_cores(self) -> int:
        cores = cpu_count() or 1
        value = ceil(cores / len(self.__call_stack)) if cores > 1 else 1
        return value

    def insert(self, data: Iterable) -> 'MultiprocessMaybe':
        last_stack_index = len(self.__call_stack)
        self.__inserted_data[last_stack_index] = data
        return self

    def then(self, func: Callable, process_count: Optional[int] = None) -> 'MultiprocessMaybe':
        self.__call_stack.append((MaybeWrapper(func, self._is_nothing).maybe, process_count))
        return self

    def _default_maybe_exec(self, value: Any) -> Any:
        return value

    def __exec_maybe(self) -> list:
        last_queue_name = QueueNames.INPUT
        for index, frame in enumerate(self.__call_stack):
            input_name = last_queue_name
            output_name = QueueNames.OUTPUT if index == len(self.__call_stack) - 1 else f'queue_{index}'
            frame_func, process_count = frame
            self.automator.register_as_worker_function(input_name, output_name, process_count or self.__balance_cores())(frame_func)
            data = self.__inserted_data.get(index)
            if data:
                self.automator.set_data_for_queue(data, input_name)
            last_queue_name = output_name

        result = self.automator.run()
        self.automator.reset()
        return result

    def maybe(self, func: Optional[Callable] = None, default: Any = None, process_count: Optional[int] = None) -> list:
        self.__call_stack.append((MaybeWrapper(func or self._default_maybe_exec, self._is_nothing, default).maybe, process_count))
        return self.__exec_maybe()
