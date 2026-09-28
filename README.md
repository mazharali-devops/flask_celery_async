````markdown
# Flask Async Celery

Run `async def` Celery tasks on a persistent asyncio event loop with bounded concurrency, Flask integration, and Celery consumer backpressure.

## Features

- Persistent `asyncio` event loop in a dedicated thread per Celery worker process.
- Run native `async def` Celery tasks.
- Bounded asynchronous concurrency with `max_tasks`.
- Celery request context propagation into the asyncio execution thread.
- `self.retry()` support for async tasks.
- Normal synchronous Celery tasks continue to work.
- Redis consumer-side backpressure through Celery's `worker_disable_prefetch`.
- Flask extension with simple configuration.
- Graceful asyncio executor shutdown.
- Compatible with Celery 5.6.x and Python 3.10+.

## Architecture

```text
                         Redis
                           │
                           ▼
                    Celery Consumer
                           │
                           │ worker_disable_prefetch
                           ▼
                      AsyncIOPool
                     max_tasks = N
                           │
                           ▼
                    AsyncExecutor
                   asyncio.Semaphore(N)
                           │
                           ▼
                  Persistent asyncio loop
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
          Async task    Async task    Async task
````

The package separates Celery's worker execution from asyncio execution:

1. Celery receives and traces the task.
2. `AsyncIOPool` bridges Celery execution into the asyncio executor.
3. `AsyncExecutor` owns a persistent asyncio event loop.
4. A semaphore limits active async tasks.
5. Celery's Redis `worker_disable_prefetch` option can prevent the consumer from reserving work beyond the available pool capacity.

## Requirements

* Python 3.10+
* Celery 5.6.x
* Flask 2.3+
* Redis when using the Redis broker/result backend and consumer backpressure.

## Installation

### From PyPI

```bash
pip install flask-async-celery
```

### With Redis support

```bash
pip install "flask-async-celery[redis]"
```

### For development and testing

```bash
pip install "flask-async-celery[test]"
```

### Development installation

Clone the repository and install it in editable mode:

```bash
git clone https://github.com/pyfuncode/flask_celery_async.git
cd flask_celery_async

pip install -e ".[test]"
```

## Basic Flask Setup

```python
import asyncio

from flask import Flask

from flask_async_celery import AsyncCelery, AsyncTask


app = Flask(__name__)

celery = AsyncCelery(
    app,
    broker_url="redis://127.0.0.1:6379/0",
    result_backend="redis://127.0.0.1:6379/1",
    max_tasks=5,
    disable_prefetch=True,
)


@celery.task(base=AsyncTask)
async def my_task(value):
    await asyncio.sleep(1)
    return value * 2
```

Send the task normally:

```python
result = my_task.delay(10)

print(result.get(timeout=30))
# 20
```

## Flask Configuration

Configuration can be supplied through Flask:

```python
app.config["ASYNC_CELERY_MAX_TASKS"] = 10
app.config["ASYNC_CELERY_DISABLE_PREFETCH"] = True

celery = AsyncCelery(
    app,
    broker_url="redis://127.0.0.1:6379/0",
    result_backend="redis://127.0.0.1:6379/1",
)
```

Available settings:

| Setting                         | Default | Description                                           |
| ------------------------------- | ------: | ----------------------------------------------------- |
| `ASYNC_CELERY_MAX_TASKS`        |    `20` | Maximum number of concurrently executing async tasks. |
| `ASYNC_CELERY_DISABLE_PREFETCH` |  `True` | Enables Celery consumer-side backpressure.            |

Constructor arguments can also be used directly:

```python
celery = AsyncCelery(
    app,
    max_tasks=10,
    disable_prefetch=True,
)
```

Flask configuration takes precedence over the constructor defaults when the extension is initialized.

## Worker Configuration

Run the worker using the package's custom pool:

```bash
celery -A your_app.celery worker \
    -P flask_async_celery.pool:AsyncIOPool \
    -c 5 \
    --loglevel=INFO
```

For example:

```bash
celery -A your_app.celery worker \
    -P flask_async_celery.pool:AsyncIOPool \
    -c 10 \
    --loglevel=INFO
```

The `-c` value should match the desired async concurrency.

The custom pool exposes its configured concurrency through `num_processes`, allowing Celery's consumer to use the same capacity when consumer-side prefetch is disabled.

## Concurrency

Set the maximum number of simultaneously executing async tasks with:

```python
celery = AsyncCelery(
    app,
    max_tasks=5,
)
```

With:

```text
max_tasks = 5
```

the asyncio executor allows at most five active coroutines at once.

Additional work waits for an available execution slot.

This is different from simply creating more threads. The package uses one persistent asyncio event loop and runs async coroutines concurrently on that loop.

## Consumer Backpressure

For Redis, Celery 5.6 supports:

```python
worker_disable_prefetch = True
```

The extension enables this by default:

```python
celery = AsyncCelery(
    app,
    max_tasks=5,
    disable_prefetch=True,
)
```

This provides two levels of protection:

```text
Celery Consumer
      │
      │ Don't reserve beyond available capacity
      ▼
 AsyncIOPool
      │
      │ max_tasks
      ▼
 AsyncExecutor
      │
      │ semaphore
      ▼
 asyncio tasks
```

You can disable the consumer-side behavior:

```python
celery = AsyncCelery(
    app,
    max_tasks=5,
    disable_prefetch=False,
)
```

When disabled, the asyncio executor still enforces its own concurrency limit.

### Redis Requirement

`worker_disable_prefetch` is intended for supported Redis worker configurations.

If you use another broker, verify that your Celery version and broker transport support this feature before relying on consumer-side backpressure.

The executor-level concurrency limit remains independent of consumer prefetch behavior.

## Async Tasks

Use `AsyncTask` as the base class for native async tasks:

```python
from flask_async_celery import AsyncTask


@celery.task(base=AsyncTask)
async def fetch_data():
    await some_async_operation()
    return "done"
```

The task can use normal Celery task features:

```python
@celery.task(
    base=AsyncTask,
    bind=True,
    max_retries=3,
)
async def process_item(self, item_id):
    try:
        return await process(item_id)
    except TemporaryError as exc:
        raise self.retry(
            exc=exc,
            countdown=5,
        )
```

The Celery request context is propagated from the Celery worker thread into the asyncio execution thread.

This means task information such as the task ID, retry count, delivery information, and retry context remains available.

## Synchronous Tasks

Normal synchronous tasks can still be used:

```python
@celery.task
def sync_task(value):
    return value * 2
```

The package does not require every task to be asynchronous.

## Retries

Async `self.retry()` is supported:

```python
@celery.task(
    base=AsyncTask,
    bind=True,
    max_retries=3,
)
async def retrying_task(self):
    if should_retry():
        raise self.retry(countdown=5)

    return "success"
```

The Celery task request is preserved when execution moves from the Celery worker thread to the asyncio event loop.

This allows Celery retry metadata and delivery information to remain available to the async task.

## Exceptions

Exceptions raised by an async task propagate through the Celery execution path:

```python
@celery.task(base=AsyncTask)
async def failing_task():
    raise RuntimeError("something went wrong")
```

Celery remains responsible for:

* task failure state
* result handling
* retry behavior
* worker-level task tracing

## Graceful Shutdown

The asyncio executor runs in a dedicated daemon thread.

When the pool stops, the executor:

1. Stops accepting new work.
2. Stops the asyncio event loop.
3. Cancels pending asyncio tasks.
4. Waits for the loop thread to terminate.
5. Closes the event loop.

## Public API

The main public API is intentionally small:

```python
from flask_async_celery import AsyncCelery, AsyncTask
```

### `AsyncCelery`

Flask integration and Celery configuration.

```python
AsyncCelery(
    app=None,
    *,
    celery=None,
    broker_url=None,
    result_backend=None,
    max_tasks=20,
    disable_prefetch=True,
)
```

### `AsyncTask`

Base class for asynchronous Celery tasks:

```python
@celery.task(base=AsyncTask)
async def my_task():
    ...
```

## Development

Clone the repository and install the project in editable mode:

```bash
git clone https://github.com/pyfuncode/flask_celery_async.git
cd flask_celery_async

pip install -e ".[test]"
```

Run the test suite:

```bash
pytest -v
```

The test suite covers:

* asyncio executor concurrency
* AsyncIOPool execution
* async task exceptions
* async retries
* synchronous retries
* real Celery worker integration
* Redis consumer backpressure
* Flask extension configuration
* end-to-end Flask/Celery/async execution

## Project Structure

```text
flask-async-celery/
├── pyproject.toml
├── README.md
├── LICENSE
├── src/
│   └── flask_async_celery/
│       ├── __init__.py
│       ├── extension.py
│       ├── executor.py
│       ├── bootstep.py
│       ├── pool.py
│       └── task.py
└── test/
    ├── conftest.py
    ├── test_executor.py
    ├── test_tasks.py
    ├── test_backpressure.py
    ├── test_celery_pool.py
    ├── test_worker_integration.py
    ├── test_extension.py
    └── test_extension_integration.py
```

## Design Notes

This package does not replace Celery's task tracing and lifecycle handling.

Celery remains responsible for:

* task delivery
* task acknowledgment
* retries
* result state
* task IDs
* worker lifecycle
* task tracing

The package provides the asyncio execution layer and integrates it with Celery's pool interface.

The asyncio event loop is persistent for the lifetime of the worker process rather than creating a new event loop for every task.

### Why a Persistent Event Loop?

Creating a new event loop for every task adds unnecessary setup and teardown overhead.

Instead, each worker process owns one persistent asyncio event loop:

```text
Celery Worker Process
│
├── Celery Consumer
├── Celery task execution
├── bridge threads
│
└── asyncio event loop thread
    ├── Task A
    ├── Task B
    └── Task C
```

Async tasks can therefore share the same event loop while still being bounded by `max_tasks`.

### Why Request Propagation?

Celery's request context is associated with the worker execution context.

The asyncio event loop runs in a separate thread, so the package explicitly transfers the current Celery request into that execution context.

This preserves information required by features such as:

```python
self.request.id
self.request.retries
self.request.delivery_info
self.retry()
```

The request is pushed before async execution and removed afterward.

## Execution Model

The execution flow can be summarized as:

```text
Celery Consumer
       │
       ▼
   AsyncIOPool
       │
       ▼
 Bridge Thread
       │
       ▼
 Celery Task
       │
       ▼
 AsyncExecutor
       │
       ▼
 Persistent asyncio Event Loop
       │
       ├── Coroutine A
       ├── Coroutine B
       └── Coroutine C
```

The bridge thread allows Celery's synchronous task execution and tracing model to interact with the asynchronous execution model.

The asyncio executor then schedules the coroutine on the persistent event loop.

## Limitations

### Broker-Specific Backpressure

Consumer-side `worker_disable_prefetch` support depends on Celery and the broker transport.

The asyncio executor's own `max_tasks` limit remains the final execution boundary.

### Worker Pool

The worker must use:

```text
flask_async_celery.pool:AsyncIOPool
```

for the package's asyncio execution model.

### One Event Loop Per Worker Process

Each worker process owns its own asyncio event loop and concurrency limit.

For example:

```bash
celery -A your_app.celery worker \
    -P flask_async_celery.pool:AsyncIOPool \
    -c 5
```

creates a worker configuration with five execution slots.

If you run multiple worker processes, each process has its own pool and event loop.

## Testing

Run the complete test suite:

```bash
pytest -v
```

The project tests the execution model with real Celery workers in addition to unit-level executor and pool tests.

The test suite covers:

* executor concurrency
* async task execution
* synchronous task execution
* async exceptions
* async retries
* synchronous retries
* Celery worker integration
* Redis consumer backpressure
* Flask extension configuration
* end-to-end Flask/Celery/async execution

A successful test run should show all tests passing.

## Version

Current version:

```text
0.1.1
```

## License

This project is licensed under the **GNU General Public License v3.0**.

Copyright (c) 2026 Mazhar Ali

This software is distributed under the terms of the GNU General Public License version 3.0.

See the [LICENSE](LICENSE) file for the complete license text.

For the full license terms, see the official GNU General Public License v3.0 text.

---

Copyright (c) 2026 Mazhar Ali

````

