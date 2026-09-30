# Changelog

## 0.2.0 - 2026-09-30

### Added

- Async task cancellation through Celery's normal termination mechanism.
- Async executor statistics.
- Running async task inspection.
- Long-running task detection.
- Configurable `slow_task_threshold`.
- `async_stats` Celery control command.
- `async_tasks` Celery control command.
- Monitoring example.

### Improved

- Async executor lifecycle and shutdown handling.
- Cancellation handling for running and queued tasks.
- Celery worker integration.
- Automatic configuration of `AsyncIOPool`.
- Documentation and worker configuration examples.

### Testing

- Added cancellation and termination tests.
- Added executor monitoring and statistics tests.
- Added long-running task detection tests.
- Added worker control command tests.