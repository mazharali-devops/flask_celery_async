## [0.2.1] - 2026-10-01

### Added

- Added Prometheus metrics support for monitoring async Celery workers.
- Added `/metrics` endpoint when Prometheus support is enabled.
- Added Prometheus and Grafana monitoring examples.
- Added Grafana dashboard with async task and worker metrics.
- Added async worker inspection commands:
  - `async_stats`
  - `async_tasks`
- Added slow-task tracking and configurable slow-task thresholds.

### Improved

- Improved async task cancellation and lifecycle handling.
- Improved worker shutdown and asyncio task cleanup.
- Improved monitoring of running, completed, failed, and cancelled tasks.
- Added Prometheus integration tests and metrics endpoint tests.
- Added `prometheus-client` to the test dependencies so CI runs the Prometheus test suite correctly.

### Documentation

- Updated README with Prometheus and Grafana setup instructions.
- Updated monitoring and cancellation documentation.
- Added monitoring example configuration and dashboard files.