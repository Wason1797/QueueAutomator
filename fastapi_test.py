import json
from fastapi import FastAPI, BackgroundTasks

from queue_automator.automator import QueueAutomator

import numpy as np
import time

import logging

logging.basicConfig(level=logging.DEBUG)

app = FastAPI()

automator = QueueAutomator()


@app.on_event('startup')
async def startup() -> None:
    automator.set_input_data([])
    automator.run(forever=True)


@app.on_event('shutdown')
async def shutdown() -> None:
    automator.kill_running_processes_and_queues()


@automator.register_as_worker_function(process_count=7)
def long_process(data: tuple) -> None:
    t0, arr, name = data
    t_offset = time.time() - t0
    dt = arr[1, 0] - arr[0, 0]
    while True:
        t = time.perf_counter_ns() - t_offset*1E9
        i = int(t/dt)
        if i >= len(arr):
            break
        arr[i, 1] += 1

    print(f'Done with {name}')
    with open(f'{name}.txt', 'w') as result:
        json.dump(arr.tolist(), result)
    return None


def process_in_automator(data: tuple) -> None:
    automator.set_data_for_queue([data], 'input', imediate=True)


@app.post('/process/queue/{data}')
async def process(data: str, tasks: BackgroundTasks) -> dict:
    tasks.add_task(process_in_automator, data=(time.time(), np.array([[0, 0], [5*1E9, 0], [10*1E9, 0], [15*1E9, 0]]), data))
    return {"message": f"processing {data}"}


@app.post('/process/no-queue/{data}')
async def process_no_queue(data: str, tasks: BackgroundTasks) -> dict:
    tasks.add_task(long_process, data=(time.time(), np.array([[0, 0], [5*1E9, 0], [10*1E9, 0], [15*1E9, 0]]), f'no_queue{data}'))
    return {"message": f"processing {data}"}
