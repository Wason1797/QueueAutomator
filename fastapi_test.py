from fastapi import FastAPI, BackgroundTasks

from queue_automator.automator import QueueAutomator

from time import sleep

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
def long_process(data: str) -> None:
    print(f'Processing: {data}')
    sleep(1)
    print(f'Done with {data}')


def process_in_automator(data: str) -> None:
    automator.set_data_for_queue([data], 'input', imediate=True)


@app.post('/process/{data}')
async def index(data: str, tasks: BackgroundTasks) -> dict:
    tasks.add_task(process_in_automator, data=data)
    return {"message": f"processing {data}"}
