#!/usr/bin/env python3
"""Label-driven Docker cron controller.

The controller watches running containers on the local Docker host and executes
commands inside containers that expose labels with the form:

    cron.<jobname>.schedule
    cron.<jobname>.command
    cron.<jobname>.jitterSeconds

It intentionally uses only Python's standard library and the Docker Engine HTTP
API over the mounted Unix socket. No Docker CLI or external scheduler is needed.
"""

from __future__ import annotations

import datetime as dt
import http.client
import json
import os
import random
import re
import signal
import shlex
import socket
import struct
import threading
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


LABEL_RE = re.compile(r"^cron\.([^.]+)\.(schedule|command|jitterSeconds|timeoutSeconds)$")
DEFAULT_DISCOVERY_INTERVAL_SECONDS = 60
DEFAULT_DOCKER_SOCKET = "/var/run/docker.sock"
DEFAULT_DOCKER_TIMEOUT_SECONDS = 30
DEFAULT_JOB_TIMEOUT_SECONDS = 3600
DEFAULT_MAX_CONCURRENT_JOBS = 10
DEFAULT_MAX_JITTER_SECONDS = 3600
DEFAULT_OUTPUT_LIMIT_BYTES = 4096
EXEC_READ_CHUNK_BYTES = 8192
EXEC_READ_IDLE_TIMEOUT_SECONDS = 1.0
USE_DEFAULT_TIMEOUT = object()


def log(level: str, message: str, **fields: Any) -> None:
    record = {
        "time": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "level": level,
        "message": message,
    }
    record.update(fields)
    print(json.dumps(record, sort_keys=True), flush=True)


class CronError(ValueError):
    pass


class DockerError(RuntimeError):
    def __init__(self, status: int, body: bytes):
        self.status = status
        self.body = body
        text = body.decode("utf-8", errors="replace").strip()
        super().__init__(f"Docker API returned {status}: {text}")


def _parse_cron_field(
    value: str,
    *,
    minimum: int,
    maximum: int,
    aliases: dict[str, int] | None = None,
    allow_seven_as_zero: bool = False,
) -> tuple[frozenset[int], bool]:
    aliases = aliases or {}
    value = value.strip().lower()
    if not value:
        raise CronError("empty cron field")

    result: set[int] = set()
    is_any = value == "*"

    for part in value.split(","):
        part = part.strip()
        if not part:
            raise CronError(f"empty list item in field {value!r}")

        if "/" in part:
            base, step_text = part.split("/", 1)
            if not step_text.isdigit():
                raise CronError(f"invalid step {step_text!r}")
            step = int(step_text)
            if step <= 0:
                raise CronError("step must be greater than zero")
        else:
            base = part
            step = 1

        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start = _parse_cron_number(start_text, aliases, allow_seven_as_zero)
            end = _parse_cron_number(end_text, aliases, allow_seven_as_zero)
            if start > end:
                raise CronError(f"range start {start} is greater than end {end}")
        else:
            start = end = _parse_cron_number(base, aliases, allow_seven_as_zero)

        if start < minimum or end > maximum:
            raise CronError(f"value {start}-{end} outside allowed range {minimum}-{maximum}")
        result.update(range(start, end + 1, step))

    return frozenset(result), is_any


def _parse_cron_number(
    value: str,
    aliases: dict[str, int],
    allow_seven_as_zero: bool,
) -> int:
    value = value.strip().lower()
    if value in aliases:
        return aliases[value]
    if not value.isdigit():
        raise CronError(f"invalid number {value!r}")
    number = int(value)
    if allow_seven_as_zero and number == 7:
        return 0
    return number


@dataclass(frozen=True)
class CronExpression:
    raw: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    day_of_month_any: bool
    day_of_week_any: bool

    @classmethod
    def parse(cls, expression: str) -> "CronExpression":
        parts = expression.split()
        if len(parts) != 5:
            raise CronError("cron expression must have exactly 5 fields")

        month_aliases = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }
        dow_aliases = {
            "sun": 0,
            "mon": 1,
            "tue": 2,
            "wed": 3,
            "thu": 4,
            "fri": 5,
            "sat": 6,
        }

        minutes, _ = _parse_cron_field(parts[0], minimum=0, maximum=59)
        hours, _ = _parse_cron_field(parts[1], minimum=0, maximum=23)
        days_of_month, dom_any = _parse_cron_field(parts[2], minimum=1, maximum=31)
        months, _ = _parse_cron_field(parts[3], minimum=1, maximum=12, aliases=month_aliases)
        days_of_week, dow_any = _parse_cron_field(
            parts[4],
            minimum=0,
            maximum=6,
            aliases=dow_aliases,
            allow_seven_as_zero=True,
        )

        return cls(
            raw=expression,
            minutes=minutes,
            hours=hours,
            days_of_month=days_of_month,
            months=months,
            days_of_week=days_of_week,
            day_of_month_any=dom_any,
            day_of_week_any=dow_any,
        )

    def matches(self, now: dt.datetime) -> bool:
        if now.minute not in self.minutes:
            return False
        if now.hour not in self.hours:
            return False
        if now.month not in self.months:
            return False

        day_of_month_matches = now.day in self.days_of_month
        cron_day_of_week = (now.weekday() + 1) % 7
        day_of_week_matches = cron_day_of_week in self.days_of_week

        if self.day_of_month_any and self.day_of_week_any:
            return True
        if self.day_of_month_any:
            return day_of_week_matches
        if self.day_of_week_any:
            return day_of_month_matches
        return day_of_month_matches or day_of_week_matches


@dataclass(frozen=True)
class Job:
    key: str
    name: str
    container_id: str
    container_name: str
    schedule: CronExpression
    command: str
    jitter_seconds: int
    timeout_seconds: int


@dataclass(frozen=True)
class ExecOutput:
    payload: bytes
    truncated: bool
    timed_out: bool


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str, timeout: float | None):
        super().__init__("localhost", timeout=timeout)
        self.socket_path = socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.socket_path)
        self.sock = sock


class DockerClient:
    def __init__(
        self,
        socket_path: str = DEFAULT_DOCKER_SOCKET,
        timeout_seconds: int = DEFAULT_DOCKER_TIMEOUT_SECONDS,
    ):
        self.socket_path = socket_path
        self.timeout_seconds = timeout_seconds

    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        timeout: float | None | object = USE_DEFAULT_TIMEOUT,
    ) -> Any:
        response = self._request(method, path, body=body, timeout=timeout)
        try:
            data = response.read()
            if response.status >= 400:
                raise DockerError(response.status, data)
            if not data:
                return None
            return json.loads(data.decode("utf-8"))
        finally:
            response.close()

    def stream(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        timeout: float | None | object = USE_DEFAULT_TIMEOUT,
    ) -> http.client.HTTPResponse:
        response = self._request(method, path, body=body, timeout=timeout)
        if response.status >= 400:
            data = response.read()
            response.close()
            raise DockerError(response.status, data)
        return response

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None,
        timeout: float | None | object,
    ) -> http.client.HTTPResponse:
        payload = None
        headers = {"Host": "docker"}
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(payload))

        conn = UnixHTTPConnection(
            self.socket_path,
            self.timeout_seconds if timeout is USE_DEFAULT_TIMEOUT else timeout,
        )
        conn.request(method, path, body=payload, headers=headers)
        return conn.getresponse()

    def list_running_containers(self) -> list[dict[str, Any]]:
        return self.request("GET", "/containers/json?all=0")

    def inspect_container(self, container_id: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(container_id, safe="")
        return self.request("GET", f"/containers/{encoded}/json")

    def container_is_running(self, container_id: str) -> bool:
        try:
            data = self.inspect_container(container_id)
        except DockerError as error:
            if error.status == 404:
                return False
            raise
        return bool(data.get("State", {}).get("Running"))

    def create_exec(self, container_id: str, argv: list[str]) -> str:
        encoded = urllib.parse.quote(container_id, safe="")
        data = self.request(
            "POST",
            f"/containers/{encoded}/exec",
            body={
                "AttachStdout": True,
                "AttachStderr": True,
                "Tty": False,
                "Cmd": argv,
            },
        )
        exec_id = data.get("Id")
        if not exec_id:
            raise RuntimeError("Docker API did not return exec id")
        return exec_id

    def start_exec(
        self,
        exec_id: str,
        *,
        timeout_seconds: int,
        output_limit_bytes: int,
    ) -> ExecOutput:
        encoded = urllib.parse.quote(exec_id, safe="")
        read_timeout = min(timeout_seconds, EXEC_READ_IDLE_TIMEOUT_SECONDS)
        response = self.stream(
            "POST",
            f"/exec/{encoded}/start",
            body={"Detach": False, "Tty": False},
            timeout=read_timeout,
        )
        try:
            return read_exec_output(response, timeout_seconds, output_limit_bytes)
        finally:
            response.close()

    def inspect_exec(self, exec_id: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(exec_id, safe="")
        return self.request("GET", f"/exec/{encoded}/json")


def parse_labels(
    *,
    container_id: str,
    container_name: str,
    labels: dict[str, str],
    max_jitter_seconds: int = DEFAULT_MAX_JITTER_SECONDS,
    max_timeout_seconds: int = DEFAULT_JOB_TIMEOUT_SECONDS,
) -> list[Job]:
    grouped: dict[str, dict[str, str]] = {}
    for key, value in labels.items():
        match = LABEL_RE.match(key)
        if not match:
            continue
        job_name, attribute = match.groups()
        grouped.setdefault(job_name, {})[attribute] = value

    jobs: list[Job] = []
    for job_name, values in sorted(grouped.items()):
        schedule_text = values.get("schedule", "").strip()
        command = values.get("command", "").strip()

        if not schedule_text or not command:
            log(
                "warning",
                "skipping incomplete job labels",
                container=container_name,
                job=job_name,
                hasSchedule=bool(schedule_text),
                hasCommand=bool(command),
            )
            continue

        try:
            schedule = CronExpression.parse(schedule_text)
        except CronError as error:
            log(
                "warning",
                "skipping job with invalid schedule",
                container=container_name,
                job=job_name,
                schedule=schedule_text,
                error=str(error),
            )
            continue

        try:
            jitter_seconds = parse_jitter(values.get("jitterSeconds", "0"))
        except ValueError as error:
            log(
                "warning",
                "skipping job with invalid jitterSeconds",
                container=container_name,
                job=job_name,
                jitterSeconds=values.get("jitterSeconds"),
                error=str(error),
            )
            continue

        if jitter_seconds > max_jitter_seconds:
            log(
                "warning",
                "capping job jitterSeconds",
                container=container_name,
                job=job_name,
                requestedJitterSeconds=jitter_seconds,
                maxJitterSeconds=max_jitter_seconds,
            )
            jitter_seconds = max_jitter_seconds

        try:
            timeout_seconds = parse_job_timeout(
                values.get("timeoutSeconds", str(max_timeout_seconds)),
            )
        except ValueError as error:
            log(
                "warning",
                "skipping job with invalid timeoutSeconds",
                container=container_name,
                job=job_name,
                timeoutSeconds=values.get("timeoutSeconds"),
                error=str(error),
            )
            continue

        if timeout_seconds > max_timeout_seconds:
            log(
                "warning",
                "capping job timeoutSeconds",
                container=container_name,
                job=job_name,
                requestedTimeoutSeconds=timeout_seconds,
                maxTimeoutSeconds=max_timeout_seconds,
            )
            timeout_seconds = max_timeout_seconds

        jobs.append(
            Job(
                key=f"{container_id}:{job_name}",
                name=job_name,
                container_id=container_id,
                container_name=container_name,
                schedule=schedule,
                command=command,
                jitter_seconds=jitter_seconds,
                timeout_seconds=timeout_seconds,
            )
        )

    return jobs


def parse_jitter(value: str) -> int:
    value = str(value).strip()
    if value == "":
        return 0
    if not value.isdigit():
        raise ValueError("jitterSeconds must be a non-negative integer")
    return int(value)


def parse_job_timeout(value: str) -> int:
    value = str(value).strip()
    if not value.isdigit():
        raise ValueError("timeoutSeconds must be a positive integer")
    timeout_seconds = int(value)
    if timeout_seconds <= 0:
        raise ValueError("timeoutSeconds must be greater than zero")
    return timeout_seconds


def parse_direct_command(command: str) -> list[str]:
    argv = shlex.split(command)
    if not argv:
        raise ValueError("command must not be empty")
    return argv


def is_missing_shell_error(error: DockerError) -> bool:
    return is_missing_shell_message(error.body.decode("utf-8", errors="replace"))


def is_missing_shell_exec_result(payload: bytes, exec_info: dict[str, Any]) -> bool:
    if exec_info.get("ExitCode") not in {126, 127}:
        return False
    stdout, stderr = decode_exec_output(payload)
    return is_missing_shell_message(f"{stdout}\n{stderr}")


def is_missing_shell_message(message: str) -> bool:
    body = message.lower()
    return "/bin/sh" in body and (
        "no such file or directory" in body
        or "executable file not found" in body
        or "stat /bin/sh" in body
    )


def container_name(container: dict[str, Any]) -> str:
    names = container.get("Names") or []
    if names:
        return str(names[0]).lstrip("/")
    return str(container.get("Id", ""))[:12]


def decode_exec_output(payload: bytes) -> tuple[str, str]:
    if not looks_like_multiplexed_exec_output(payload):
        return payload.decode("utf-8", errors="replace").strip(), ""

    stdout: list[bytes] = []
    stderr: list[bytes] = []
    index = 0

    while index + 8 <= len(payload):
        stream_type = payload[index]
        size = struct.unpack(">I", payload[index + 4 : index + 8])[0]
        index += 8
        chunk = payload[index : index + size]
        if len(chunk) != size:
            break
        if stream_type == 1:
            stdout.append(chunk)
        elif stream_type == 2:
            stderr.append(chunk)
        else:
            stdout.append(chunk)
        index += size

    if index < len(payload):
        stdout.append(payload[index:])

    return (
        b"".join(stdout).decode("utf-8", errors="replace").strip(),
        b"".join(stderr).decode("utf-8", errors="replace").strip(),
    )


def looks_like_multiplexed_exec_output(payload: bytes) -> bool:
    if len(payload) < 8:
        return False
    stream_type = payload[0]
    if stream_type not in {1, 2}:
        return False
    if payload[1:4] != b"\x00\x00\x00":
        return False
    size = struct.unpack(">I", payload[4:8])[0]
    return size <= len(payload) - 8


def read_exec_output(
    response: http.client.HTTPResponse,
    timeout_seconds: int,
    output_limit_bytes: int,
) -> ExecOutput:
    deadline = time.monotonic() + timeout_seconds
    payload = bytearray()
    truncated = False
    read_chunk = getattr(response, "read1", response.read)

    while True:
        if time.monotonic() >= deadline:
            return ExecOutput(bytes(payload), truncated, True)

        try:
            chunk = read_chunk(EXEC_READ_CHUNK_BYTES)
        except (TimeoutError, socket.timeout):
            continue

        if not chunk:
            return ExecOutput(bytes(payload), truncated, False)

        available = output_limit_bytes - len(payload)
        if available > 0:
            payload.extend(chunk[:available])
        if len(chunk) > available:
            truncated = True


class DockerCron:
    def __init__(
        self,
        *,
        docker: DockerClient,
        timezone: dt.tzinfo,
        discovery_interval_seconds: int,
        self_container_id: str | None,
        job_timeout_seconds: int = DEFAULT_JOB_TIMEOUT_SECONDS,
        max_concurrent_jobs: int = DEFAULT_MAX_CONCURRENT_JOBS,
        max_jitter_seconds: int = DEFAULT_MAX_JITTER_SECONDS,
        output_limit_bytes: int = DEFAULT_OUTPUT_LIMIT_BYTES,
        log_job_output: bool = True,
    ):
        self.docker = docker
        self.timezone = timezone
        self.discovery_interval_seconds = discovery_interval_seconds
        self.self_container_id = self_container_id
        self.job_timeout_seconds = job_timeout_seconds
        self.max_concurrent_jobs = max_concurrent_jobs
        self.max_jitter_seconds = max_jitter_seconds
        self.output_limit_bytes = output_limit_bytes
        self.log_job_output = log_job_output
        self.jobs: dict[str, Job] = {}
        self.last_run: dict[str, str] = {}
        self.active_jobs: set[str] = set()
        self.job_slots = threading.BoundedSemaphore(max_concurrent_jobs)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.rescan_event = threading.Event()

    def run(self) -> None:
        log(
            "info",
            "starting docker-cron",
            discoveryIntervalSeconds=self.discovery_interval_seconds,
            jobTimeoutSeconds=self.job_timeout_seconds,
            maxConcurrentJobs=self.max_concurrent_jobs,
            maxJitterSeconds=self.max_jitter_seconds,
            outputLimitBytes=self.output_limit_bytes,
            logJobOutput=self.log_job_output,
        )

        self.scan()
        scanner = threading.Thread(target=self._scan_loop, name="scanner", daemon=True)
        events = threading.Thread(target=self._events_loop, name="docker-events", daemon=True)
        scanner.start()
        events.start()

        try:
            self._schedule_loop()
        finally:
            self.stop_event.set()
            self.rescan_event.set()

    def stop(self) -> None:
        self.stop_event.set()
        self.rescan_event.set()

    def _scan_loop(self) -> None:
        while not self.stop_event.is_set():
            self.scan()
            self.rescan_event.wait(self.discovery_interval_seconds)
            self.rescan_event.clear()

    def _events_loop(self) -> None:
        filters = urllib.parse.quote(json.dumps({"type": ["container"]}))
        path = f"/events?filters={filters}"

        while not self.stop_event.is_set():
            try:
                response = self.docker.stream("GET", path, timeout=None)
                try:
                    while not self.stop_event.is_set():
                        line = response.readline()
                        if not line:
                            break
                        event = json.loads(line.decode("utf-8"))
                        action = event.get("Action") or event.get("status")
                        if action in {"start", "die", "stop", "kill", "destroy"}:
                            self.rescan_event.set()
                finally:
                    response.close()
            except Exception as error:
                log("warning", "docker events stream disconnected", error=str(error))
                self.stop_event.wait(5)

    def scan(self) -> None:
        try:
            containers = self.docker.list_running_containers()
        except Exception as error:
            log("error", "failed to list containers", error=str(error))
            return

        next_jobs: dict[str, Job] = {}
        for container in containers:
            container_id = str(container.get("Id", ""))
            if not container_id:
                continue
            if self._is_self(container_id):
                continue
            labels = container.get("Labels") or {}
            parsed_jobs = parse_labels(
                container_id=container_id,
                container_name=container_name(container),
                labels={str(key): str(value) for key, value in labels.items()},
                max_jitter_seconds=self.max_jitter_seconds,
                max_timeout_seconds=self.job_timeout_seconds,
            )
            for job in parsed_jobs:
                next_jobs[job.key] = job

        with self.lock:
            previous_keys = set(self.jobs)
            next_keys = set(next_jobs)
            self.jobs = next_jobs
            self.last_run = {
                key: minute
                for key, minute in self.last_run.items()
                if key in next_keys
            }

        for key in sorted(next_keys - previous_keys):
            job = next_jobs[key]
            log(
                "info",
                "registered job",
                container=job.container_name,
                job=job.name,
                schedule=job.schedule.raw,
                jitterSeconds=job.jitter_seconds,
                timeoutSeconds=job.timeout_seconds,
            )

        for key in sorted(previous_keys - next_keys):
            log("info", "unregistered job", key=key)

        log("info", "container scan complete", containers=len(containers), jobs=len(next_jobs))

    def _schedule_loop(self) -> None:
        while not self.stop_event.is_set():
            now = dt.datetime.now(self.timezone).replace(second=0, microsecond=0)
            minute_key = now.isoformat()
            due_jobs = self._due_jobs(now, minute_key)

            for job in due_jobs:
                if not self._reserve_job_slot(job):
                    continue
                thread = threading.Thread(
                    target=self._run_job_thread,
                    args=(job, minute_key),
                    name=f"job-{job.name}",
                    daemon=True,
                )
                thread.start()

            next_minute = now + dt.timedelta(minutes=1)
            sleep_for = max(0.1, (next_minute - dt.datetime.now(self.timezone)).total_seconds())
            self.stop_event.wait(sleep_for)

    def _due_jobs(self, now: dt.datetime, minute_key: str) -> list[Job]:
        with self.lock:
            jobs = list(self.jobs.values())
            due_jobs = []
            for job in jobs:
                if self.last_run.get(job.key) == minute_key:
                    continue
                if not job.schedule.matches(now):
                    continue
                self.last_run[job.key] = minute_key
                due_jobs.append(job)
            return due_jobs

    def _reserve_job_slot(self, job: Job) -> bool:
        with self.lock:
            if job.key in self.active_jobs:
                log(
                    "warning",
                    "skipping job because previous execution is still running",
                    container=job.container_name,
                    job=job.name,
                )
                return False
            if not self.job_slots.acquire(blocking=False):
                log(
                    "warning",
                    "skipping job because max concurrent jobs is reached",
                    container=job.container_name,
                    job=job.name,
                    maxConcurrentJobs=self.max_concurrent_jobs,
                )
                return False
            self.active_jobs.add(job.key)
            return True

    def _release_job_slot(self, job: Job) -> None:
        with self.lock:
            self.active_jobs.remove(job.key)
            self.job_slots.release()

    def _run_job_thread(self, job: Job, minute_key: str) -> None:
        try:
            self._run_job(job, minute_key)
        finally:
            self._release_job_slot(job)

    def _run_job(self, job: Job, minute_key: str) -> None:
        if job.jitter_seconds > 0:
            delay = random.randint(0, job.jitter_seconds)
            log(
                "info",
                "waiting before job execution",
                container=job.container_name,
                job=job.name,
                scheduledMinute=minute_key,
                delaySeconds=delay,
            )
            if self.stop_event.wait(delay):
                return

        log(
            "info",
            "starting job",
            container=job.container_name,
            job=job.name,
            scheduledMinute=minute_key,
            command=job.command,
        )

        try:
            if not self.docker.container_is_running(job.container_id):
                log(
                    "warning",
                    "skipping job because container is not running",
                    container=job.container_name,
                    job=job.name,
                )
                self.rescan_event.set()
                return

            output, exec_info, exec_mode = self._execute_job_command(job)
            exit_code = exec_info.get("ExitCode")
            log_fields = {
                "container": job.container_name,
                "job": job.name,
                "exitCode": exit_code,
                "execMode": exec_mode,
                "timedOut": output.timed_out,
                "outputTruncated": output.truncated,
            }
            if self.log_job_output:
                stdout, stderr = decode_exec_output(output.payload)
                log_fields.update(stdout=stdout, stderr=stderr)

            log(
                "info" if exit_code == 0 and not output.timed_out else "error",
                "job finished",
                **log_fields,
            )
        except Exception as error:
            log(
                "error",
                "job execution failed",
                container=job.container_name,
                job=job.name,
                error=str(error),
            )
            self.rescan_event.set()

    def _execute_job_command(self, job: Job) -> tuple[ExecOutput, dict[str, Any], str]:
        try:
            output, exec_info = self._execute_argv(job, ["/bin/sh", "-lc", job.command])
        except DockerError as error:
            if not is_missing_shell_error(error):
                raise
        else:
            if output.timed_out or not is_missing_shell_exec_result(output.payload, exec_info):
                return output, exec_info, "shell"

        argv = parse_direct_command(job.command)
        log(
            "warning",
            "container has no /bin/sh, retrying job with direct exec",
            container=job.container_name,
            job=job.name,
            argv=argv,
        )
        return (*self._execute_argv(job, argv), "direct")

    def _execute_argv(self, job: Job, argv: list[str]) -> tuple[ExecOutput, dict[str, Any]]:
        exec_id = self.docker.create_exec(job.container_id, argv)
        output = self.docker.start_exec(
            exec_id,
            timeout_seconds=job.timeout_seconds,
            output_limit_bytes=self.output_limit_bytes,
        )
        exec_info = self.docker.inspect_exec(exec_id)
        return output, exec_info

    def _is_self(self, container_id: str) -> bool:
        if not self.self_container_id:
            return False
        return container_id.startswith(self.self_container_id) or self.self_container_id.startswith(
            container_id[:12]
        )


def read_timezone() -> dt.tzinfo:
    timezone_name = os.environ.get("TZ", "UTC")
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        log("warning", "timezone not found, falling back to UTC", timezone=timezone_name)
        return dt.timezone.utc


def read_positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be an integer")
    if value <= 0:
        raise SystemExit(f"{name} must be greater than zero")
    return value


def read_non_negative_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be an integer")
    if value < 0:
        raise SystemExit(f"{name} must be zero or greater")
    return value


def read_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise SystemExit(f"{name} must be a boolean")


def main() -> int:
    docker_socket = os.environ.get("DOCKER_SOCKET", DEFAULT_DOCKER_SOCKET)
    discovery_interval_seconds = read_positive_int(
        "DISCOVERY_INTERVAL_SECONDS",
        DEFAULT_DISCOVERY_INTERVAL_SECONDS,
    )
    timeout_seconds = read_positive_int(
        "DOCKER_TIMEOUT_SECONDS",
        DEFAULT_DOCKER_TIMEOUT_SECONDS,
    )
    job_timeout_seconds = read_positive_int(
        "JOB_TIMEOUT_SECONDS",
        DEFAULT_JOB_TIMEOUT_SECONDS,
    )
    max_concurrent_jobs = read_positive_int(
        "MAX_CONCURRENT_JOBS",
        DEFAULT_MAX_CONCURRENT_JOBS,
    )
    max_jitter_seconds = read_non_negative_int(
        "MAX_JITTER_SECONDS",
        DEFAULT_MAX_JITTER_SECONDS,
    )
    output_limit_bytes = read_non_negative_int(
        "OUTPUT_LIMIT_BYTES",
        DEFAULT_OUTPUT_LIMIT_BYTES,
    )
    log_job_output = read_bool("LOG_JOB_OUTPUT", True)
    self_container_id = os.environ.get("SELF_CONTAINER_ID") or socket.gethostname()

    controller = DockerCron(
        docker=DockerClient(docker_socket, timeout_seconds),
        timezone=read_timezone(),
        discovery_interval_seconds=discovery_interval_seconds,
        self_container_id=self_container_id,
        job_timeout_seconds=job_timeout_seconds,
        max_concurrent_jobs=max_concurrent_jobs,
        max_jitter_seconds=max_jitter_seconds,
        output_limit_bytes=output_limit_bytes,
        log_job_output=log_job_output,
    )

    def handle_signal(signum: int, _frame: Any) -> None:
        log("info", "received shutdown signal", signal=signum)
        controller.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    controller.run()
    return 0


def entrypoint() -> None:
    raise SystemExit(main())


if __name__ == "__main__":  # pragma: no cover
    entrypoint()  # pragma: no cover
