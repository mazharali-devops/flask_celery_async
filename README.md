# Flask Async Celery

Run native `async def` Celery tasks on a persistent asyncio event loop with bounded concurrency, Flask integration, and Celery consumer-side backpressure.

## Features

- Persistent `asyncio` event loop in a dedicated thread per Celery worker process.
- Run native `async def` Celery tasks.
- Bounded asynchronous concurrency with `max_tasks`.
- Celery task request context propagation into the asyncio execution thread.
- `self.retry()` support for async tasks.
- Normal synchronous Celery tasks continue to work.
- Redis consumer-side backpressure through Celery's `worker_disable_prefetch`.
- Flask extension with simple configuration.
- Graceful asyncio executor shutdown.
- Task cancellation and termination through Celery's normal task lifecycle.
- Async executor monitoring and long-running task detection.
- Custom Celery control commands for async executor statistics and running tasks.
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
                     Bridge Threads
                           │
                           ▼
                     AsyncExecutor
                    asyncio.Semaphore(N)
                           │
                           ▼
                  Persistent asyncio loop
                           │
               ┌───────────┼───────────┐
               ▼           ▼           ▼
           Async task  Async task  Async task
```

The package separates Celery's worker execution model from asyncio execution:

1. Celery receives and traces the task.
2. `AsyncIOPool` bridges Celery execution into the asyncio executor.
3. `AsyncExecutor` owns a persistent asyncio event loop.
4. An asyncio semaphore limits the number of actively executing async tasks.
5. Celery's Redis `worker_disable_prefetch` option can reduce unnecessary task reservation when consumer-side backpressure is enabled.

The package does **not** manually pause and resume the Celery consumer. Consumer-side backpressure is provided through Celery's supported `worker_disable_prefetch` behavior.

## Requirements

- Python 3.10+
- Celery 5.6.x
- Flask 2.3+
- Redis when using the Redis broker/result backend and consumer-side backpressure

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

### With Prometheus monitoring

```bash
pip install "flask-async-celery[prometheus]"
```

This installs `prometheus-client` for the built-in `/metrics` endpoint.

### Development installation

Clone the repository and install it in editable mode:

```bash
git clone https://github.com/mazharali-devops/flask_celery_async.git
cd flask_celery_async

pip install -e ".[test]"
```

## Basic Flask Setup

```python
import asyncio

from flask import Flask

from flask_async_celery import AsyncCelery


app = Flask(__name__)

celery = AsyncCelery(
    app,
    broker_url="redis://127.0.0.1:6379/0",
    result_backend="redis://127.0.0.1:6379/1",
    max_tasks=5,
    disable_prefetch=True,
)


@celery.task
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

`AsyncCelery.task()` automatically uses `AsyncTask` for native async task functions. You can also specify `base=AsyncTask` explicitly when you want to make the task base visible:

```python
from flask_async_celery import AsyncTask


@celery.task(base=AsyncTask)
async def another_task():
    await asyncio.sleep(1)
    return "done"
```

## Flask Configuration

Configuration can be supplied through Flask:

```python
app.config["ASYNC_CELERY_MAX_TASKS"] = 10
app.config["ASYNC_CELERY_DISABLE_PREFETCH"] = True
app.config["ASYNC_CELERY_SLOW_TASK_THRESHOLD"] = 30.0

celery = AsyncCelery(
    app,
    broker_url="redis://127.0.0.1:6379/0",
    result_backend="redis://127.0.0.1:6379/1",
)
```

Available settings:

| Setting | Default | Description |
| --- | ---: | --- |
| `ASYNC_CELERY_MAX_TASKS` | `20` | Maximum number of concurrently executing async tasks. |
| `ASYNC_CELERY_DISABLE_PREFETCH` | `True` | Enables Celery consumer-side backpressure where supported. |
| `ASYNC_CELERY_SLOW_TASK_THRESHOLD` | `30.0` | Seconds after which a running async task is considered long-running. |

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

`AsyncCelery` configures the Celery worker to use the package's `AsyncIOPool` automatically.

For normal usage, start the worker with:

```bash
celery -A your_app.celery worker --loglevel=INFO
```

You normally do **not** need to pass `-P` or `--pool` manually, and you do not need to pass `-c` to configure async concurrency.

Configure async concurrency through `AsyncCelery`:

```python
celery = AsyncCelery(
    app,
    max_tasks=5,
)
```

`max_tasks` is the package's async execution limit for each Celery worker process.

The extension also configures Celery's worker concurrency from `max_tasks`, so the worker and async executor use the same configured capacity.

The custom pool exposes its configured concurrency through `num_processes`, allowing Celery's consumer to use the same capacity when consumer-side prefetch is disabled.

### Monitoring example

The repository includes an example application under `example/monitoring`. Start it with:

```bash
celery -A example.monitoring.main:celery_app worker --loglevel=INFO
```

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

Each Celery worker process has its own asyncio executor and event loop.

## Monitoring

The package exposes async executor statistics through Celery's worker inspection system.

The standard Celery inspect command includes the async metrics in the worker statistics:

```bash
celery -A example.monitoring.main:celery_app inspect stats
```

The async statistics include:

- `asyncio-enabled`
- `asyncio-running`
- `asyncio-available`
- `asyncio-max-tasks`
- `asyncio-completed`
- `asyncio-failed`
- `asyncio-cancelled`
- `asyncio-total`
- `asyncio-average-duration`
- `asyncio-slow-task-threshold`
- `asyncio-long-running`
- `asyncio-long-running-tasks`
- `asyncio-event-loop-running`
- `asyncio-stopping`

The package also registers two custom Celery control commands:

```python
replies = celery_app.control.broadcast(
    "async_stats",
    reply=True,
)

replies = celery_app.control.broadcast(
    "async_tasks",
    reply=True,
)
```

`async_stats` returns async executor statistics. `async_tasks` returns the currently running async tasks.

These custom commands are invoked through Celery's Python control API; they are **not** standard `celery inspect <command>` CLI subcommands.

### Long-running task threshold

The default long-running threshold is 30 seconds. Configure it with `slow_task_threshold`:

```python
celery = AsyncCelery(
    app,
    max_tasks=5,
    slow_task_threshold=60.0,
)
```

Or through Flask configuration:

```python
app.config["ASYNC_CELERY_SLOW_TASK_THRESHOLD"] = 60.0
```

A running task is reported as long-running when its execution duration is above the configured threshold.

### Prometheus metrics

Prometheus monitoring is optional and disabled by default.

Enable it in Flask:

```python
app.config["PROMETHEUS_ENABLED"] = True
```

When enabled, the extension registers:

```text
GET /metrics
```

The endpoint exports metrics for each responding Celery worker, including:

- currently running async tasks
- available async execution slots
- maximum async concurrency
- completed tasks
- failed tasks
- cancelled tasks
- average task duration
- long-running tasks
- configured slow-task threshold
- asyncio event-loop status
- executor stopping status

Example:

```python
import asyncio

from flask import Flask
from flask_async_celery import AsyncCelery

app = Flask(__name__)
app.config["PROMETHEUS_ENABLED"] = True

celery = AsyncCelery(
    app,
    broker_url="redis://127.0.0.1:6379/0",
    result_backend="redis://127.0.0.1:6379/1",
    max_tasks=5,
)
```

Prometheus can scrape the endpoint with:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "flask-async-celery"
    metrics_path: /metrics
    static_configs:
      - targets:
          - "127.0.0.1:5000"
```

The repository includes this configuration at:

```text
example/monitoring/prometheus.yml
```

The metrics collector uses the package's `async_stats` Celery control command to retrieve worker statistics. If no workers respond, the endpoint exports no async worker metrics.

### Grafana dashboard

A ready-to-import Grafana dashboard is included at:

```text
example/monitoring/grafana-dashboard.json
```

The dashboard includes panels for:

- Running Async Tasks
- Available Async Slots
- Max Async Concurrency
- Long Running Tasks
- Completed Tasks
- Failed Tasks
- Cancelled Tasks
- AsyncIO Event Loop
- Running Tasks Over Time
- Available Slots Over Time
- Task Completion Rate
- Task Failure Rate
- Average Task Duration
- Slow Task Threshold

Basic monitoring setup:

```text
Flask application
      │
      ├── /metrics
      │
      ▼
 Prometheus
      │
      ▼
   Grafana
```

Start the monitoring worker:

```bash
celery -A example.monitoring.main:celery_app worker --loglevel=INFO
```

Start the Flask application using the monitoring example's application entry point, configure Prometheus to scrape its `/metrics` endpoint, and import `example/monitoring/grafana-dashboard.json` into Grafana.

## Consumer Backpressure

For Redis, Celery 5.6 supports:

```python
worker_disable_prefetch = True
```

The extension enables this behavior by default:

```python
celery = AsyncCelery(
    app,
    max_tasks=5,
    disable_prefetch=True,
)
```

When supported by the broker transport, this provides consumer-side backpressure in addition to the executor's own concurrency limit:

```text
Celery Consumer
      │
      │ worker_disable_prefetch
      │
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

The executor's semaphore remains the final execution boundary.

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

The executor-level `max_tasks` limit remains independent of consumer prefetch behavior.

The package does not manually pause or resume the Celery consumer.

## Async Tasks

Native async tasks can be declared directly with `@celery.task`:

```python
@celery.task
async def fetch_data():
    await some_async_operation()
    return "done"
```

The extension automatically uses `AsyncTask` for async task functions.

You can also explicitly specify the task base:

```python
from flask_async_celery import AsyncTask


@celery.task(base=AsyncTask)
async def fetch_data():
    await some_async_operation()
    return "done"
```

### Celery Task Features

Async tasks can use normal Celery task features such as bound tasks and retries:

```python
@celery.task(
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

## Celery Request Context

The package propagates the Celery task request from the Celery worker execution thread into the asyncio execution thread.

This preserves Celery request information such as:

```python
self.request.id
self.request.retries
self.request.delivery_info
```

and allows features such as:

```python
self.retry()
```

to continue working for async tasks.

The request is pushed before async execution and removed afterward.

### Flask HTTP Request Context

Celery task request propagation is different from Flask HTTP request-context propagation.

The package does **not** keep a Flask HTTP request context alive while a background Celery task executes.

If a background task needs information from an HTTP request, pass that information explicitly as task arguments.

For example:

```python
@celery.task
async def process_user(user_id, request_id):
    ...
```

This is preferable to depending on the lifetime of the original HTTP request.

## Synchronous Tasks

Normal synchronous Celery tasks can still be used:

```python
@celery.task
def sync_task(value):
    return value * 2
```

The package does not require every task to be asynchronous.

Synchronous tasks continue through the normal Celery task execution path.

## Retries

Async `self.retry()` is supported:

```python
@celery.task(
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

Exceptions raised by an async task propagate through the normal Celery execution path:

```python
@celery.task
async def failing_task():
    raise RuntimeError("something went wrong")
```

Celery remains responsible for:

- task failure state
- result handling
- retry behavior
- worker-level task tracing

The package provides the asyncio execution layer without replacing Celery's task tracing and lifecycle handling.

## Graceful Shutdown

The asyncio executor runs in a dedicated daemon thread.

During normal pool shutdown, the executor:

1. Stops accepting new work.
2. Stops the asyncio event loop.
3. Cancels pending asyncio tasks.
4. Waits for the loop thread when requested.
5. Closes the asyncio event loop.

The pool and executor remain responsible for their own lifecycle cleanup.

## Public API

The main public API is intentionally small:

```python
from flask_async_celery import AsyncCelery, AsyncTask
```

### `AsyncCelery`

Provides Flask integration and Celery configuration:

```python
AsyncCelery(
    app=None,
    *,
    celery=None,
    broker_url=None,
    result_backend=None,
    max_tasks=20,
    disable_prefetch=True,
    slow_task_threshold=30.0,
)
```

### `AsyncTask`

Base class for asynchronous Celery tasks:

```python
@celery.task(base=AsyncTask)
async def my_task():
    ...
```

In most cases you can simply use:

```python
@celery.task
async def my_task():
    ...
```

because `AsyncCelery` automatically uses `AsyncTask` for async tasks.

## Development

Clone the repository and install the project in editable mode:

```bash
git clone https://github.com/mazharali-devops/flask_celery_async.git
cd flask_celery_async

pip install -e ".[test]"
```

Run the test suite:

```bash
pytest -v
```

The test suite covers:

- asyncio executor concurrency
- asyncio executor lifecycle and shutdown
- `AsyncIOPool` execution
- async task exceptions
- async retries
- running async task cancellation
- queued async task cancellation
- Celery task termination
- synchronous task execution
- synchronous retries
- Celery worker integration
- Redis consumer backpressure
- Celery Hub wakeup handling
- Flask extension configuration
- Flask application-context isolation
- end-to-end Flask/Celery/async execution
- async executor statistics
- long-running task detection
- worker monitoring and control commands
- Prometheus metric collection
- Prometheus `/metrics` endpoint
- Grafana dashboard configuration

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
│       ├── hub_bootstep.py
│       ├── hub_wakeup.py
│       ├── metrics.py
│       ├── pool.py
│       └── task.py
├── example/
│   └── monitoring/
│       ├── __init__.py
│       ├── grafana-dashboard.json
│       ├── main.py
│       └── prometheus.yml
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

- task delivery
- task acknowledgment
- retries
- result state
- task IDs
- worker lifecycle
- task tracing

The package provides the asyncio execution layer and integrates it with Celery's pool interface.

### Persistent Event Loop

The asyncio event loop is persistent for the lifetime of the worker process rather than creating a new event loop for every task.

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

### Request Propagation

Celery's task request is associated with the worker execution context.

The asyncio event loop runs in a separate thread, so the package explicitly transfers the current Celery request into that execution context.

This preserves information required by features such as:

```python
self.request.id
self.request.retries
self.request.delivery_info
self.retry()
```

The request is pushed before async execution and removed afterward.

This is Celery task-request propagation, not Flask HTTP request-context propagation.

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

The bridge thread allows Celery's synchronous execution and tracing model to interact with the asynchronous execution model.

The asyncio executor then schedules the coroutine on the persistent event loop.

The bridge thread waits for the asyncio execution to complete so that Celery can continue using its normal synchronous task execution and callback model.

## Limitations

### Broker-Specific Backpressure

Consumer-side `worker_disable_prefetch` support depends on Celery and the broker transport.

The asyncio executor's own `max_tasks` limit remains the final execution boundary.

If consumer-side prefetch control is unavailable for a broker, the executor still prevents more than `max_tasks` async tasks from actively executing.

### Worker Pool

`AsyncCelery` configures the worker to use:

```text
flask_async_celery.pool:AsyncIOPool
```

automatically. For normal usage, do not specify `-P` or `--pool` manually:

```bash
celery -A your_app.celery worker --loglevel=INFO
```

Configure the async capacity with `max_tasks` when creating `AsyncCelery`.

### One Event Loop Per Worker Process

Each worker process owns its own asyncio event loop and concurrency limit.

For example, configuring `max_tasks=5` creates five async execution slots in that worker process.

If you run multiple worker processes, each process has its own pool, bridge threads, and asyncio event loop.

The total async capacity is therefore distributed across the worker processes.

### HTTP Request Data

A Celery task should not depend on the lifetime of the Flask HTTP request that originally triggered it.

Pass required request-specific information explicitly to the task.

### Task Cancellation and Termination

Async tasks can be terminated through Celery's normal task termination mechanism. For example:

```python
result = my_task.delay()
result.revoke(terminate=True)
```

The termination signal is propagated to the corresponding asyncio task. The asyncio task is cancelled, its executor slot is released, and Celery reports the task using its normal lifecycle and state handling.

The package does not replace Celery's task state, acknowledgement, retry, or result handling. Celery remains responsible for the task lifecycle while the async executor handles cancellation of the underlying asyncio task.

## Testing

Run the complete test suite:

```bash
pytest -v
```

The project tests the execution model with real Celery workers in addition to unit-level executor and pool tests.

The test suite covers:

- executor concurrency
- executor lifecycle
- async task execution
- synchronous task execution
- async exceptions
- async retries
- synchronous retries
- Celery worker integration
- Redis consumer backpressure
- Celery Hub wakeup behavior
- Flask extension configuration
- Flask application-context handling
- end-to-end Flask/Celery/async execution

A successful test run should show all tests passing.

## Version

Current version:

```text
0.2.1
```

## License

This project is licensed under the **GNU General Public License v3.0**.

Copyright (c) 2026 Mazhar Ali

This software is distributed under the terms of the GNU General Public License version 3.0.

See the [LICENSE](LICENSE) file for the complete license text.

For the full license terms, see the official GNU General Public License v3.0 text.
