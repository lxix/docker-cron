# docker-cron

`docker-cron` runs cron-like jobs inside already running Docker containers based
on container labels.

The controller uses the Docker Engine API through `/var/run/docker.sock`. The
runtime logic is a Python standard-library script.

## Labels

```yaml
labels:
  cron.<jobname>.schedule: "<cron expression>"
  cron.<jobname>.jitterSeconds: "<max jitter in seconds>"
  cron.<jobname>.timeoutSeconds: "<max execution wait in seconds>"
  cron.<jobname>.command: "<command>"
```

Required labels:

- `cron.<jobname>.schedule`
- `cron.<jobname>.command`

Optional labels:

- `cron.<jobname>.jitterSeconds`, defaults to `0`
- `cron.<jobname>.timeoutSeconds`, defaults to `JOB_TIMEOUT_SECONDS`

Example:

```yaml
services:
  wordpress:
    image: bitnami/wordpress
    labels:
      cron.wp.schedule: "*/5 * * * *"
      cron.wp.jitterSeconds: "60"
      cron.wp.timeoutSeconds: "300"
      cron.wp.command: "wp --path=/bitnami/wordpress cron event run --due-now"
```

Job names may not contain dots.

## Cron syntax

The scheduler accepts five-field cron expressions:

```text
minute hour day-of-month month day-of-week
```

Supported forms:

- `*`
- `*/5`
- `1,2,3`
- `10-20`
- `10-20/2`
- month aliases like `jan`, `feb`
- weekday aliases like `sun`, `mon`

When both day-of-month and day-of-week are restricted, the job runs when either
field matches, matching traditional cron behavior.

## Run

```yaml
services:
  docker-cron:
    image: ghcr.io/lxix/docker-cron:latest
    restart: unless-stopped
    environment:
      TZ: Europe/Budapest
      DISCOVERY_INTERVAL_SECONDS: "60"
      JOB_TIMEOUT_SECONDS: "3600"
      MAX_CONCURRENT_JOBS: "10"
      MAX_JITTER_SECONDS: "3600"
      OUTPUT_LIMIT_BYTES: "4096"
      LOG_JOB_OUTPUT: "true"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

Build locally:

```sh
docker build -t docker-cron:latest .
```

## Release image

Images are published to GitHub Container Registry when a GitHub release is
published or a version tag is pushed:

```sh
git tag v1.0.0
git push origin v1.0.0
```

The release workflow publishes these tags:

- `ghcr.io/lxix/docker-cron:latest`
- `ghcr.io/lxix/docker-cron:1.0.0`

## Behavior

- On startup, the controller scans all currently running containers.
- It watches Docker container events and rescans after `start`, `stop`, `die`,
  `kill`, and `destroy` events.
- It also performs a full periodic rescan, controlled by
  `DISCOVERY_INTERVAL_SECONDS`.
- Jobs do not overlap with a previous still-running execution of the same job.
- The controller enforces a global `MAX_CONCURRENT_JOBS` limit.
- A job that exceeds its timeout is logged as timed out. The controller keeps
  that job slot reserved until Docker reports that the exec process has
  finished, or the target container stops, so timed-out jobs do not pile up with
  overlapping retries. Use command-level timeouts too when hard process
  cancellation matters.
- Job output is captured up to `OUTPUT_LIMIT_BYTES` and marked as truncated when
  the command writes more. Set `LOG_JOB_OUTPUT=false` to omit stdout/stderr from
  controller logs.
- Commands are executed inside the labeled container with:

```sh
/bin/sh -lc "<command>"
```

If the target image has no `/bin/sh`, the controller retries the command as a
direct executable invocation parsed with shell-like quoting rules. For example,
`/whoami --help` becomes `["/whoami", "--help"]`.

## Configuration

Environment variables:

- `TZ`: timezone used for schedule evaluation, default `UTC`
- `DISCOVERY_INTERVAL_SECONDS`: periodic full rescan interval, default `60`
- `DOCKER_SOCKET`: Docker Unix socket path, default `/var/run/docker.sock`
- `DOCKER_TIMEOUT_SECONDS`: Docker API request timeout, default `30`
- `JOB_TIMEOUT_SECONDS`: default and maximum per-job execution wait timeout,
  default `3600`
- `MAX_CONCURRENT_JOBS`: maximum concurrently running job workers, default `10`
- `MAX_JITTER_SECONDS`: maximum accepted per-job jitter, default `3600`
- `OUTPUT_LIMIT_BYTES`: maximum captured job output bytes, default `4096`
- `LOG_JOB_OUTPUT`: include captured stdout/stderr in logs, default `true`
- `SELF_CONTAINER_ID`: optional explicit id/prefix for the controller container

## Security

Mounting `/var/run/docker.sock` grants the controller root-equivalent control
over the Docker host. Treat container labels as a trusted control plane: any
actor that can create or change `cron.*` labels can make the controller execute
commands inside those containers. Only run this on trusted hosts, with trusted
compose files or deploy manifests, and avoid using it across isolation
boundaries where untrusted users can create containers.
