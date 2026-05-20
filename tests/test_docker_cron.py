import contextlib
import datetime as dt
import io
import json
import os
import signal
import struct
import unittest
from unittest import mock

import docker_cron as dc
from docker_cron import (
    CronError,
    CronExpression,
    DockerClient,
    DockerCron,
    DockerError,
    ExecOutput,
    Job,
    UnixHTTPConnection,
    container_name,
    decode_exec_output,
    is_missing_shell_error,
    is_missing_shell_exec_result,
    looks_like_multiplexed_exec_output,
    parse_direct_command,
    parse_jitter,
    parse_job_timeout,
    parse_labels,
    read_bool,
    read_non_negative_int,
    read_positive_int,
    read_timezone,
)


def quiet_logs():
    return contextlib.redirect_stdout(io.StringIO())


def make_job(
    *,
    key="container123:job",
    name="job",
    container_id="container123",
    container_name="target",
    schedule="* * * * *",
    command="echo ok",
    jitter_seconds=0,
    timeout_seconds=60,
):
    return Job(
        key=key,
        name=name,
        container_id=container_id,
        container_name=container_name,
        schedule=CronExpression.parse(schedule),
        command=command,
        jitter_seconds=jitter_seconds,
        timeout_seconds=timeout_seconds,
    )


class FakeResponse:
    def __init__(self, status=200, body=b"", lines=None, on_empty=None):
        self.status = status
        self.body = body
        self.body_offset = 0
        self.lines = list(lines or [])
        self.on_empty = on_empty
        self.closed = False
        self.read_called = False

    def read(self, size=None):
        self.read_called = True
        if self.body_offset >= len(self.body):
            return b""
        if size is None:
            chunk = self.body[self.body_offset :]
            self.body_offset = len(self.body)
            return chunk
        chunk = self.body[self.body_offset : self.body_offset + size]
        self.body_offset += len(chunk)
        return chunk

    def readline(self):
        if self.lines:
            return self.lines.pop(0)
        if self.on_empty:
            self.on_empty()
        return b""

    def close(self):
        self.closed = True


class FakeLoopEvent:
    def __init__(self):
        self.flag = False
        self.waited = []
        self.set_called = False
        self.clear_called = False

    def is_set(self):
        return self.flag

    def wait(self, timeout=None):
        self.waited.append(timeout)
        self.flag = True
        return False

    def set(self):
        self.flag = True
        self.set_called = True

    def clear(self):
        self.clear_called = True


class RecordingEvent:
    def __init__(self, wait_results=None):
        self.wait_results = list(wait_results or [])
        self.waited = []
        self.flag = False

    def is_set(self):
        return self.flag

    def wait(self, timeout=None):
        self.waited.append(timeout)
        if self.wait_results:
            return self.wait_results.pop(0)
        return False

    def set(self):
        self.flag = True


class CronExpressionTests(unittest.TestCase):
    def test_every_minute_matches(self):
        cron = CronExpression.parse("* * * * *")
        self.assertTrue(cron.matches(dt.datetime(2026, 5, 20, 12, 34)))

    def test_step_matches_only_expected_minutes(self):
        cron = CronExpression.parse("*/15 * * * *")
        self.assertTrue(cron.matches(dt.datetime(2026, 5, 20, 12, 30)))
        self.assertFalse(cron.matches(dt.datetime(2026, 5, 20, 12, 31)))

    def test_range_with_step(self):
        cron = CronExpression.parse("10-20/5 * * * *")
        self.assertTrue(cron.matches(dt.datetime(2026, 5, 20, 12, 10)))
        self.assertTrue(cron.matches(dt.datetime(2026, 5, 20, 12, 15)))
        self.assertFalse(cron.matches(dt.datetime(2026, 5, 20, 12, 11)))

    def test_weekday_alias(self):
        cron = CronExpression.parse("0 9 * * wed")
        self.assertTrue(cron.matches(dt.datetime(2026, 5, 20, 9, 0)))
        self.assertFalse(cron.matches(dt.datetime(2026, 5, 21, 9, 0)))

    def test_sunday_can_be_zero_or_seven(self):
        at_sunday = dt.datetime(2026, 5, 24, 9, 0)
        self.assertTrue(CronExpression.parse("0 9 * * 0").matches(at_sunday))
        self.assertTrue(CronExpression.parse("0 9 * * 7").matches(at_sunday))

    def test_day_of_month_or_day_of_week_semantics(self):
        cron = CronExpression.parse("0 9 1 * wed")
        self.assertTrue(cron.matches(dt.datetime(2026, 5, 1, 9, 0)))
        self.assertTrue(cron.matches(dt.datetime(2026, 5, 20, 9, 0)))
        self.assertFalse(cron.matches(dt.datetime(2026, 5, 21, 9, 0)))

    def test_rejects_invalid_expression(self):
        with self.assertRaises(CronError):
            CronExpression.parse("* * * *")

    def test_rejects_invalid_field_parts(self):
        invalid_expressions = [
            "",
            "1,,2",
            "*/x",
            "*/0",
            "5-2",
            "60",
            "abc",
        ]
        for expression in invalid_expressions:
            with self.subTest(expression=expression):
                with self.assertRaises(CronError):
                    dc._parse_cron_field(expression, minimum=0, maximum=59)

    def test_hour_month_and_day_of_month_only_matching(self):
        self.assertFalse(CronExpression.parse("0 9 * * *").matches(dt.datetime(2026, 5, 20, 8, 0)))
        self.assertFalse(CronExpression.parse("0 9 * jun *").matches(dt.datetime(2026, 5, 20, 9, 0)))

        cron = CronExpression.parse("0 9 20 * *")
        self.assertTrue(cron.matches(dt.datetime(2026, 5, 20, 9, 0)))
        self.assertFalse(cron.matches(dt.datetime(2026, 5, 21, 9, 0)))

    def test_month_and_weekday_aliases(self):
        cron = CronExpression.parse("0 9 * may mon")
        self.assertTrue(cron.matches(dt.datetime(2026, 5, 25, 9, 0)))


class LabelParsingTests(unittest.TestCase):
    def test_parse_complete_job(self):
        jobs = parse_labels(
            container_id="abc",
            container_name="demo",
            labels={
                "cron.echo.schedule": "* * * * *",
                "cron.echo.command": "echo test",
                "cron.echo.jitterSeconds": "5",
                "cron.echo.timeoutSeconds": "30",
            },
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].name, "echo")
        self.assertEqual(jobs[0].command, "echo test")
        self.assertEqual(jobs[0].jitter_seconds, 5)
        self.assertEqual(jobs[0].timeout_seconds, 30)

    def test_ignores_old_job_prefix(self):
        jobs = parse_labels(
            container_id="abc",
            container_name="demo",
            labels={
                "job.echo.schedule": "* * * * *",
                "job.echo.command": "echo test",
            },
        )
        self.assertEqual(jobs, [])

    def test_incomplete_job_is_skipped(self):
        with quiet_logs():
            jobs = parse_labels(
                container_id="abc",
                container_name="demo",
                labels={"cron.echo.schedule": "* * * * *"},
            )
        self.assertEqual(jobs, [])

    def test_invalid_schedule_is_skipped(self):
        with quiet_logs():
            jobs = parse_labels(
                container_id="abc",
                container_name="demo",
                labels={
                    "cron.echo.schedule": "* * * *",
                    "cron.echo.command": "echo test",
                },
            )
        self.assertEqual(jobs, [])

    def test_invalid_label_jitter_is_skipped(self):
        with quiet_logs():
            jobs = parse_labels(
                container_id="abc",
                container_name="demo",
                labels={
                    "cron.echo.schedule": "* * * * *",
                    "cron.echo.command": "echo test",
                    "cron.echo.jitterSeconds": "bad",
                },
            )
        self.assertEqual(jobs, [])

    def test_empty_jitter_defaults_to_zero(self):
        self.assertEqual(parse_jitter(""), 0)

    def test_invalid_jitter_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_jitter("-1")

    def test_jitter_and_timeout_are_capped(self):
        with quiet_logs():
            jobs = parse_labels(
                container_id="abc",
                container_name="demo",
                labels={
                    "cron.echo.schedule": "* * * * *",
                    "cron.echo.command": "echo test",
                    "cron.echo.jitterSeconds": "50",
                    "cron.echo.timeoutSeconds": "70",
                },
                max_jitter_seconds=5,
                max_timeout_seconds=60,
            )

        self.assertEqual(jobs[0].jitter_seconds, 5)
        self.assertEqual(jobs[0].timeout_seconds, 60)

    def test_invalid_timeout_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_job_timeout("0")
        with self.assertRaises(ValueError):
            parse_job_timeout("bad")

        with quiet_logs():
            jobs = parse_labels(
                container_id="abc",
                container_name="demo",
                labels={
                    "cron.echo.schedule": "* * * * *",
                    "cron.echo.command": "echo test",
                    "cron.echo.timeoutSeconds": "bad",
                },
            )
        self.assertEqual(jobs, [])


class CommandParsingTests(unittest.TestCase):
    def test_parse_direct_command_preserves_quotes(self):
        self.assertEqual(
            parse_direct_command('/whoami --name "cron probe"'),
            ["/whoami", "--name", "cron probe"],
        )

    def test_parse_direct_command_rejects_empty_command(self):
        with self.assertRaises(ValueError):
            parse_direct_command("")

    def test_missing_shell_error_is_detected(self):
        error = DockerError(500, b'exec: "/bin/sh": stat /bin/sh: no such file or directory')
        self.assertTrue(is_missing_shell_error(error))

    def test_missing_shell_error_variants_and_false_case(self):
        executable_error = DockerError(500, b'exec: "/bin/sh": executable file not found')
        stat_error = DockerError(500, b'exec: "/bin/sh": stat /bin/sh failed')
        other_error = DockerError(500, b"permission denied")
        self.assertTrue(is_missing_shell_error(executable_error))
        self.assertTrue(is_missing_shell_error(stat_error))
        self.assertFalse(is_missing_shell_error(other_error))

    def test_missing_shell_exec_result_detection(self):
        payload = b'OCI runtime exec failed: exec: "/bin/sh": stat /bin/sh: no such file or directory'
        self.assertTrue(is_missing_shell_exec_result(payload, {"ExitCode": 127}))
        self.assertFalse(is_missing_shell_exec_result(payload, {"ExitCode": 1}))
        self.assertFalse(is_missing_shell_exec_result(b"ordinary command failure", {"ExitCode": 127}))


class DockerOutputTests(unittest.TestCase):
    def test_decode_plain_output(self):
        stdout, stderr = decode_exec_output(b"plain output")
        self.assertEqual(stdout, "plain output")
        self.assertEqual(stderr, "")

    def test_decode_multiplexed_output(self):
        payload = (
            b"\x01\x00\x00\x00" + struct.pack(">I", 3) + b"out"
            + b"\x02\x00\x00\x00" + struct.pack(">I", 3) + b"err"
            + b"\x03\x00\x00\x00" + struct.pack(">I", 5) + b"extra"
        )
        stdout, stderr = decode_exec_output(payload)
        self.assertEqual(stdout, "outextra")
        self.assertEqual(stderr, "err")

    def test_decode_truncated_multiplexed_output_keeps_remainder(self):
        payload = (
            b"\x01\x00\x00\x00" + struct.pack(">I", 2) + b"ok"
            + b"\x02\x00\x00\x00" + struct.pack(">I", 10) + b"short"
        )
        stdout, stderr = decode_exec_output(payload)
        self.assertEqual(stdout, "okshort")
        self.assertEqual(stderr, "")

    def test_multiplex_detection_false_cases(self):
        self.assertFalse(looks_like_multiplexed_exec_output(b""))
        self.assertFalse(looks_like_multiplexed_exec_output(b"\x03\x00\x00\x00\x00\x00\x00\x00"))
        self.assertFalse(looks_like_multiplexed_exec_output(b"\x01bad\x00\x00\x00\x00"))
        self.assertFalse(looks_like_multiplexed_exec_output(b"\x01\x00\x00\x00\x00\x00\x00\x02x"))

    def test_read_exec_output_limits_output(self):
        output = dc.read_exec_output(FakeResponse(body=b"abcdef"), timeout_seconds=10, output_limit_bytes=3)
        self.assertEqual(output, ExecOutput(b"abc", truncated=True, timed_out=False))

    def test_read_exec_output_times_out(self):
        class TimeoutResponse:
            def read(self, _size):
                raise TimeoutError()

        with mock.patch.object(dc.time, "monotonic", side_effect=[0, 0, 2]):
            output = dc.read_exec_output(TimeoutResponse(), timeout_seconds=1, output_limit_bytes=3)

        self.assertEqual(output, ExecOutput(b"", truncated=False, timed_out=True))

    def test_read_exec_output_handles_timed_out_file_object(self):
        class TimedOutFileResponse:
            def read(self, _size):
                raise OSError("cannot read from timed out object")

        with mock.patch.object(dc.time, "monotonic", side_effect=[0, 0, 2]):
            output = dc.read_exec_output(TimedOutFileResponse(), timeout_seconds=1, output_limit_bytes=3)

        self.assertEqual(output, ExecOutput(b"", truncated=False, timed_out=True))

    def test_read_exec_output_reraises_unexpected_os_errors(self):
        class BrokenResponse:
            def read(self, _size):
                raise OSError("broken pipe")

        with self.assertRaises(OSError):
            dc.read_exec_output(BrokenResponse(), timeout_seconds=1, output_limit_bytes=3)


class DockerHttpTests(unittest.TestCase):
    def test_unix_connection_connects_to_socket_path(self):
        class FakeSocket:
            def __init__(self, family, kind):
                self.family = family
                self.kind = kind
                self.timeout = None
                self.connected_to = None

            def settimeout(self, timeout):
                self.timeout = timeout

            def connect(self, path):
                self.connected_to = path

        created = []

        def fake_socket(family, kind):
            sock = FakeSocket(family, kind)
            created.append(sock)
            return sock

        with mock.patch.object(dc.socket, "socket", fake_socket):
            connection = UnixHTTPConnection("/tmp/docker.sock", timeout=12)
            connection.connect()

        self.assertEqual(created[0].family, dc.socket.AF_UNIX)
        self.assertEqual(created[0].kind, dc.socket.SOCK_STREAM)
        self.assertEqual(created[0].timeout, 12)
        self.assertEqual(created[0].connected_to, "/tmp/docker.sock")
        self.assertIs(connection.sock, created[0])

    def test_request_decodes_json_empty_body_and_errors(self):
        class FakeConnection:
            responses = []
            instances = []

            def __init__(self, socket_path, timeout):
                self.socket_path = socket_path
                self.timeout = timeout
                self.request_args = None
                FakeConnection.instances.append(self)

            def request(self, method, path, body=None, headers=None):
                self.request_args = (method, path, body, headers)

            def getresponse(self):
                return FakeConnection.responses.pop(0)

        ok_response = FakeResponse(body=b'{"ok": true}')
        empty_response = FakeResponse(body=b"")
        error_response = FakeResponse(status=500, body=b"broken")
        FakeConnection.responses = [ok_response, empty_response, error_response]

        with mock.patch.object(dc, "UnixHTTPConnection", FakeConnection):
            client = DockerClient("/sock", timeout_seconds=9)
            self.assertEqual(client.request("POST", "/x", body={"a": 1}, timeout=None), {"ok": True})
            self.assertIsNone(client.request("GET", "/empty"))
            with self.assertRaises(DockerError):
                client.request("GET", "/error")

        first = FakeConnection.instances[0]
        self.assertEqual(first.socket_path, "/sock")
        self.assertIsNone(first.timeout)
        self.assertEqual(first.request_args[0], "POST")
        self.assertEqual(first.request_args[1], "/x")
        self.assertEqual(json.loads(first.request_args[2].decode("utf-8")), {"a": 1})
        self.assertEqual(first.request_args[3]["Content-Type"], "application/json")
        self.assertEqual(FakeConnection.instances[1].timeout, 9)
        self.assertTrue(ok_response.closed)
        self.assertTrue(empty_response.closed)
        self.assertTrue(error_response.closed)

    def test_stream_returns_response_and_closes_error_response(self):
        class FakeConnection:
            responses = []

            def __init__(self, socket_path, timeout):
                self.socket_path = socket_path
                self.timeout = timeout

            def request(self, method, path, body=None, headers=None):
                self.request_args = (method, path, body, headers)

            def getresponse(self):
                return FakeConnection.responses.pop(0)

        ok_response = FakeResponse(body=b"stream")
        error_response = FakeResponse(status=404, body=b"missing")
        FakeConnection.responses = [ok_response, error_response]

        with mock.patch.object(dc, "UnixHTTPConnection", FakeConnection):
            client = DockerClient("/sock")
            self.assertIs(client.stream("GET", "/events"), ok_response)
            with self.assertRaises(DockerError):
                client.stream("GET", "/events")

        self.assertFalse(ok_response.closed)
        self.assertTrue(error_response.closed)

    def test_docker_client_wrappers(self):
        client = DockerClient()
        client.request = mock.Mock()
        client.stream = mock.Mock()

        client.request.return_value = [{"Id": "one"}]
        self.assertEqual(client.list_running_containers(), [{"Id": "one"}])

        client.request.return_value = {"State": {"Running": True}}
        self.assertEqual(client.inspect_container("a/b"), {"State": {"Running": True}})
        self.assertTrue(client.container_is_running("a/b"))

        client.inspect_container = mock.Mock(return_value={"State": {"Running": False}})
        self.assertFalse(client.container_is_running("stopped"))
        client.inspect_container = mock.Mock(side_effect=DockerError(404, b"gone"))
        self.assertFalse(client.container_is_running("missing"))
        client.inspect_container = mock.Mock(side_effect=DockerError(500, b"broken"))
        with self.assertRaises(DockerError):
            client.container_is_running("broken")

        client.request = mock.Mock(return_value={"Id": "exec123"})
        self.assertEqual(client.create_exec("a/b", ["echo", "ok"]), "exec123")
        self.assertIn("/containers/a%2Fb/exec", client.request.call_args.args)

        client.request = mock.Mock(return_value={})
        with self.assertRaises(RuntimeError):
            client.create_exec("abc", ["echo"])

        response = FakeResponse(body=b"payload")
        client.stream = mock.Mock(return_value=response)
        self.assertEqual(
            client.start_exec("exec/1", timeout_seconds=3, output_limit_bytes=10),
            ExecOutput(b"payload", truncated=False, timed_out=False),
        )
        self.assertEqual(client.stream.call_args.kwargs["timeout"], 1.0)
        self.assertTrue(response.closed)

        client.request = mock.Mock(return_value={"ExitCode": 0})
        self.assertEqual(client.inspect_exec("exec/1"), {"ExitCode": 0})


class DockerCronTests(unittest.TestCase):
    def test_scan_registers_and_unregisters_jobs(self):
        class FakeDocker:
            def list_running_containers(self):
                return [
                    {"Id": "", "Names": [], "Labels": {}},
                    {
                        "Id": "self-container-id",
                        "Names": ["/docker-cron"],
                        "Labels": {"cron.skip.schedule": "* * * * *", "cron.skip.command": "echo skip"},
                    },
                    {
                        "Id": "container123",
                        "Names": ["/whoami"],
                        "Labels": {
                            "cron.echo.schedule": "* * * * *",
                            "cron.echo.command": "echo ok",
                            "cron.echo.jitterSeconds": 2,
                        },
                    },
                ]

        controller = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id="self",
        )
        controller.jobs = {"old:gone": make_job(key="old:gone")}
        controller.last_run = {"old:gone": "old", "container123:echo": "current"}

        with quiet_logs():
            controller.scan()

        self.assertEqual(set(controller.jobs), {"container123:echo"})
        self.assertEqual(controller.jobs["container123:echo"].container_name, "whoami")
        self.assertEqual(controller.last_run, {"container123:echo": "current"})

    def test_scan_logs_list_errors(self):
        class FakeDocker:
            def list_running_containers(self):
                raise RuntimeError("socket down")

        controller = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        with quiet_logs():
            controller.scan()

        self.assertEqual(controller.jobs, {})

    def test_due_jobs_returns_only_unstarted_matching_jobs_without_marking(self):
        docker = mock.Mock()
        docker.list_running_containers.return_value = []
        controller = DockerCron(
            docker=docker,
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        due = make_job(key="due", schedule="0 9 * * *")
        duplicate = make_job(key="duplicate", schedule="0 9 * * *")
        not_due = make_job(key="not_due", schedule="1 9 * * *")
        controller.jobs = {job.key: job for job in [due, duplicate, not_due]}
        controller.last_run = {"duplicate": "2026-05-20T09:00:00+00:00"}

        jobs = controller._due_jobs(
            dt.datetime(2026, 5, 20, 9, 0, tzinfo=dt.timezone.utc),
            "2026-05-20T09:00:00+00:00",
        )

        self.assertEqual(jobs, [due])
        self.assertNotIn("due", controller.last_run)
        self.assertEqual(controller.last_run["duplicate"], "2026-05-20T09:00:00+00:00")

    def test_reserve_job_slot_blocks_overlap_and_concurrency(self):
        controller = DockerCron(
            docker=mock.Mock(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
            max_concurrent_jobs=1,
        )
        first = make_job(key="first")
        second = make_job(key="second")

        self.assertTrue(controller._reserve_job_slot(first))
        with quiet_logs():
            self.assertFalse(controller._reserve_job_slot(first))
            self.assertFalse(controller._reserve_job_slot(second))
        controller._release_job_slot(first)
        self.assertTrue(controller._reserve_job_slot(second))
        controller._release_job_slot(second)

    def test_run_job_thread_releases_reserved_slot(self):
        docker = mock.Mock()
        docker.list_running_containers.return_value = []
        controller = DockerCron(
            docker=docker,
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        job = make_job()
        self.assertTrue(controller._reserve_job_slot(job))
        controller._run_job = mock.Mock()

        controller._run_job_thread(job, "minute")

        self.assertEqual(controller.active_jobs, set())
        controller._run_job.assert_called_once_with(job, "minute")

    def test_run_starts_worker_threads_and_sets_stop_flags(self):
        class FakeThread:
            created = []

            def __init__(self, target, name, daemon):
                self.target = target
                self.name = name
                self.daemon = daemon
                self.started = False
                FakeThread.created.append(self)

            def start(self):
                self.started = True

        docker = mock.Mock()
        docker.list_running_containers.return_value = []
        controller = DockerCron(
            docker=docker,
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        controller._schedule_loop = mock.Mock()

        with mock.patch.object(dc.threading, "Thread", FakeThread), quiet_logs():
            controller.run()

        self.assertEqual([thread.name for thread in FakeThread.created], ["scanner", "docker-events"])
        self.assertTrue(all(thread.started for thread in FakeThread.created))
        self.assertTrue(controller.stop_event.is_set())
        self.assertTrue(controller.rescan_event.is_set())

    def test_stop_sets_stop_and_rescan_events(self):
        controller = DockerCron(
            docker=mock.Mock(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )

        controller.stop()

        self.assertTrue(controller.stop_event.is_set())
        self.assertTrue(controller.rescan_event.is_set())

    def test_scan_loop_runs_scan_waits_and_clears_rescan_event(self):
        class RescanEvent(FakeLoopEvent):
            def __init__(self, stop_event):
                super().__init__()
                self.stop_event = stop_event

            def wait(self, timeout=None):
                waited = super().wait(timeout)
                self.stop_event.set()
                return waited

        controller = DockerCron(
            docker=mock.Mock(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=7,
            self_container_id=None,
        )
        stop_event = FakeLoopEvent()
        rescan_event = RescanEvent(stop_event)
        controller.stop_event = stop_event
        controller.rescan_event = rescan_event
        controller.scan = mock.Mock()

        controller._scan_loop()

        controller.scan.assert_called_once_with()
        self.assertEqual(rescan_event.waited, [7])
        self.assertTrue(rescan_event.clear_called)

    def test_events_loop_reads_events_and_sets_rescan(self):
        stop_event = dc.threading.Event()
        rescan_event = dc.threading.Event()
        lines = [
            json.dumps({"Action": "start"}).encode("utf-8") + b"\n",
            json.dumps({"status": "die"}).encode("utf-8") + b"\n",
            json.dumps({"Action": "noop"}).encode("utf-8") + b"\n",
        ]
        response = FakeResponse(lines=lines, on_empty=stop_event.set)
        docker = mock.Mock()
        docker.stream.return_value = response
        controller = DockerCron(
            docker=docker,
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        controller.stop_event = stop_event
        controller.rescan_event = rescan_event

        controller._events_loop()

        self.assertTrue(response.closed)
        self.assertTrue(rescan_event.is_set())
        docker.stream.assert_called_once()

    def test_events_loop_logs_stream_errors_and_waits(self):
        class StopAfterWait:
            def __init__(self):
                self.flag = False
                self.waited = None

            def is_set(self):
                return self.flag

            def wait(self, timeout):
                self.waited = timeout
                self.flag = True
                return False

        docker = mock.Mock()
        docker.stream.side_effect = RuntimeError("stream failed")
        controller = DockerCron(
            docker=docker,
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        stop_event = StopAfterWait()
        controller.stop_event = stop_event

        with quiet_logs():
            controller._events_loop()

        self.assertEqual(stop_event.waited, 5)

    def test_schedule_loop_starts_due_job_threads_once(self):
        class StopAfterWait(FakeLoopEvent):
            pass

        class FakeThread:
            created = []

            def __init__(self, target, args, name, daemon):
                self.target = target
                self.args = args
                self.name = name
                self.daemon = daemon
                self.started = False
                FakeThread.created.append(self)

            def start(self):
                self.started = True

        controller = DockerCron(
            docker=mock.Mock(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        job = make_job(name="tick")
        controller.jobs = {job.key: job}
        controller.stop_event = StopAfterWait()

        with mock.patch.object(dc.threading, "Thread", FakeThread):
            controller._schedule_loop()

        self.assertEqual(len(FakeThread.created), 1)
        self.assertEqual(FakeThread.created[0].name, "job-tick")
        self.assertTrue(FakeThread.created[0].started)
        self.assertIn(job.key, controller.last_run)

    def test_schedule_loop_skips_when_job_slot_is_unavailable(self):
        controller = DockerCron(
            docker=mock.Mock(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        job = make_job(name="tick")
        controller.jobs = {job.key: job}
        controller.stop_event = FakeLoopEvent()
        controller._reserve_job_slot = mock.Mock(return_value=False)

        with mock.patch.object(dc.threading, "Thread") as thread_class:
            controller._schedule_loop()

        thread_class.assert_not_called()
        self.assertNotIn(job.key, controller.last_run)
        self.assertEqual(controller.stop_event.waited, [1.0])

    def test_run_job_success_nonzero_stopped_and_error_paths(self):
        class FakeDocker:
            def __init__(self, *, running=True, exit_code=0, fail_create=False):
                self.running = running
                self.exit_code = exit_code
                self.fail_create = fail_create

            def container_is_running(self, container_id):
                return self.running

            def create_exec(self, container_id, argv):
                if self.fail_create:
                    raise RuntimeError("create failed")
                return "exec-id"

            def start_exec(self, exec_id, *, timeout_seconds, output_limit_bytes):
                return ExecOutput(b"job output", truncated=False, timed_out=False)

            def inspect_exec(self, exec_id):
                return {"ExitCode": self.exit_code}

        success = DockerCron(
            docker=FakeDocker(exit_code=0),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        with mock.patch.object(dc.random, "randint", return_value=0), quiet_logs():
            success._run_job(make_job(jitter_seconds=1), "minute")

        failed_exit = DockerCron(
            docker=FakeDocker(exit_code=2),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        with quiet_logs():
            failed_exit._run_job(make_job(), "minute")

        stopped = DockerCron(
            docker=FakeDocker(running=False),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        with quiet_logs():
            stopped._run_job(make_job(), "minute")
        self.assertTrue(stopped.rescan_event.is_set())

        errored = DockerCron(
            docker=FakeDocker(fail_create=True),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        with quiet_logs():
            errored._run_job(make_job(), "minute")
        self.assertTrue(errored.rescan_event.is_set())

        interrupted = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        interrupted.stop_event.set()
        with mock.patch.object(dc.random, "randint", return_value=5), quiet_logs():
            interrupted._run_job(make_job(jitter_seconds=5), "minute")

    def test_run_job_waits_for_timed_out_exec_in_calling_thread(self):
        class FakeDocker:
            def __init__(self):
                self.inspect_results = [
                    {"Running": True, "ExitCode": None},
                    {"Running": True, "ExitCode": None},
                    {"Running": False, "ExitCode": 124},
                ]

            def container_is_running(self, container_id):
                return True

            def create_exec(self, container_id, argv):
                return "exec-timeout"

            def start_exec(self, exec_id, *, timeout_seconds, output_limit_bytes):
                return ExecOutput(b"slow", truncated=False, timed_out=True)

            def inspect_exec(self, exec_id):
                return self.inspect_results.pop(0)

        controller = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        controller.stop_event = RecordingEvent()

        with quiet_logs():
            controller._run_job(make_job(), "minute")

        self.assertEqual(controller.stop_event.waited, [dc.EXEC_TIMEOUT_POLL_INTERVAL_SECONDS])

    def test_run_job_thread_keeps_slot_until_timed_out_exec_finishes(self):
        class BlockingEvent:
            def __init__(self):
                self.flag = False
                self.entered_wait = dc.threading.Event()
                self.release_wait = dc.threading.Event()
                self.waited = []

            def is_set(self):
                return self.flag

            def wait(self, timeout=None):
                self.waited.append(timeout)
                self.entered_wait.set()
                self.release_wait.wait(1)
                return False

            def set(self):
                self.flag = True
                self.release_wait.set()

        class FakeDocker:
            def __init__(self):
                self.inspect_results = [
                    {"Running": True, "ExitCode": None},
                    {"Running": True, "ExitCode": None},
                    {"Running": False, "ExitCode": 124},
                ]

            def container_is_running(self, container_id):
                return True

            def create_exec(self, container_id, argv):
                return "exec-timeout"

            def start_exec(self, exec_id, *, timeout_seconds, output_limit_bytes):
                return ExecOutput(b"slow", truncated=False, timed_out=True)

            def inspect_exec(self, exec_id):
                return self.inspect_results.pop(0)

        controller = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        controller.stop_event = BlockingEvent()
        job = make_job()
        self.assertTrue(controller._reserve_job_slot(job))

        with quiet_logs():
            thread = dc.threading.Thread(target=controller._run_job_thread, args=(job, "minute"))
            thread.start()
            self.assertTrue(controller.stop_event.entered_wait.wait(1))
            self.assertIn(job.key, controller.active_jobs)
            self.assertFalse(controller._reserve_job_slot(job))
            controller.stop_event.release_wait.set()
            thread.join(1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(controller.active_jobs, set())
        self.assertEqual(controller.stop_event.waited, [dc.EXEC_TIMEOUT_POLL_INTERVAL_SECONDS])

    def test_wait_for_timed_out_exec_handles_missing_exec(self):
        class FakeDocker:
            def inspect_exec(self, exec_id):
                raise DockerError(404, b"missing")

        controller = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )

        with quiet_logs():
            controller._wait_for_timed_out_exec(make_job(), "missing-exec")

    def test_wait_for_timed_out_exec_retries_inspect_errors(self):
        class FakeDocker:
            def __init__(self):
                self.results = [
                    DockerError(500, b"broken"),
                    {"Running": False, "ExitCode": 1},
                ]

            def inspect_exec(self, exec_id):
                result = self.results.pop(0)
                if isinstance(result, Exception):
                    raise result
                return result

        controller = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        controller.stop_event = RecordingEvent()

        with quiet_logs():
            controller._wait_for_timed_out_exec(make_job(), "flaky-exec")

        self.assertEqual(controller.stop_event.waited, [dc.EXEC_TIMEOUT_POLL_INTERVAL_SECONDS])

    def test_wait_for_timed_out_exec_stops_after_inspect_error_when_requested(self):
        class FakeDocker:
            def inspect_exec(self, exec_id):
                raise DockerError(500, b"broken")

        controller = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        controller.stop_event = RecordingEvent(wait_results=[True])

        with quiet_logs():
            controller._wait_for_timed_out_exec(make_job(), "broken-exec")

        self.assertEqual(controller.stop_event.waited, [dc.EXEC_TIMEOUT_POLL_INTERVAL_SECONDS])

    def test_wait_for_timed_out_exec_returns_when_container_stops(self):
        class FakeDocker:
            def inspect_exec(self, exec_id):
                return {"Running": True, "ExitCode": None}

            def container_is_running(self, container_id):
                return False

        controller = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )

        with quiet_logs():
            controller._wait_for_timed_out_exec(make_job(), "orphaned-exec")

        self.assertTrue(controller.rescan_event.is_set())

    def test_wait_for_timed_out_exec_returns_when_stop_is_requested(self):
        class FakeDocker:
            def inspect_exec(self, exec_id):
                return {"Running": True, "ExitCode": None}

            def container_is_running(self, container_id):
                return True

        controller = DockerCron(
            docker=FakeDocker(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        controller.stop_event = RecordingEvent(wait_results=[True])

        with quiet_logs():
            controller._wait_for_timed_out_exec(make_job(), "stopping-exec")

        self.assertEqual(controller.stop_event.waited, [dc.EXEC_TIMEOUT_POLL_INTERVAL_SECONDS])

    def test_execute_job_command_shell_direct_and_error_modes(self):
        class FakeDocker:
            def __init__(self, create_effects, start_payloads=None, inspect_results=None):
                self.create_effects = list(create_effects)
                self.start_payloads = list(start_payloads or [b"payload"] * len(create_effects))
                self.inspect_results = list(inspect_results or [{"ExitCode": 0}] * len(create_effects))
                self.argvs = []

            def create_exec(self, container_id, argv):
                self.argvs.append(argv)
                effect = self.create_effects.pop(0)
                if isinstance(effect, Exception):
                    raise effect
                return effect

            def start_exec(self, exec_id, *, timeout_seconds, output_limit_bytes):
                payload = self.start_payloads.pop(0)
                if isinstance(payload, ExecOutput):
                    return payload
                return ExecOutput(payload, truncated=False, timed_out=False)

            def inspect_exec(self, exec_id):
                return self.inspect_results.pop(0)

        job = make_job(command='/whoami --name "cron probe"')

        shell_controller = DockerCron(
            docker=FakeDocker(["shell-exec"]),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        exec_id, output, exec_info, mode = shell_controller._execute_job_command(job)
        self.assertEqual(exec_id, "shell-exec")
        self.assertEqual((output, exec_info, mode), (ExecOutput(b"payload", False, False), {"ExitCode": 0}, "shell"))

        missing_shell = DockerError(500, b'exec: "/bin/sh": stat /bin/sh: no such file or directory')
        direct_docker = FakeDocker([missing_shell, "direct-exec"])
        direct_controller = DockerCron(
            docker=direct_docker,
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        with quiet_logs():
            exec_id, output, exec_info, mode = direct_controller._execute_job_command(job)
        self.assertEqual(exec_id, "direct-exec")
        self.assertEqual(mode, "direct")
        self.assertEqual(direct_docker.argvs[1], ["/whoami", "--name", "cron probe"])

        start_failure_docker = FakeDocker(
            ["shell-exec", "direct-exec"],
            start_payloads=[
                b'OCI runtime exec failed: exec: "/bin/sh": stat /bin/sh: no such file or directory',
                b"direct payload",
            ],
            inspect_results=[{"ExitCode": 127}, {"ExitCode": 0}],
        )
        start_failure_controller = DockerCron(
            docker=start_failure_docker,
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        with quiet_logs():
            exec_id, output, exec_info, mode = start_failure_controller._execute_job_command(job)
        self.assertEqual(exec_id, "direct-exec")
        self.assertEqual((output, exec_info, mode), (ExecOutput(b"direct payload", False, False), {"ExitCode": 0}, "direct"))
        self.assertEqual(start_failure_docker.argvs[1], ["/whoami", "--name", "cron probe"])

        timeout_controller = DockerCron(
            docker=FakeDocker(["shell-exec"], start_payloads=[ExecOutput(b"slow", False, True)]),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        exec_id, output, exec_info, mode = timeout_controller._execute_job_command(job)
        self.assertEqual(exec_id, "shell-exec")
        self.assertEqual((output, exec_info, mode), (ExecOutput(b"slow", False, True), {"ExitCode": 0}, "shell"))

        non_missing = DockerError(500, b"permission denied")
        error_controller = DockerCron(
            docker=FakeDocker([non_missing]),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        with self.assertRaises(DockerError):
            error_controller._execute_job_command(job)

    def test_is_self_variants(self):
        controller = DockerCron(
            docker=mock.Mock(),
            timezone=dt.timezone.utc,
            discovery_interval_seconds=60,
            self_container_id=None,
        )
        self.assertFalse(controller._is_self("container"))

        controller.self_container_id = "abcdef"
        self.assertTrue(controller._is_self("abcdef123456"))
        self.assertTrue(controller._is_self("abcdef"))
        self.assertFalse(controller._is_self("123456"))


class ConfigurationTests(unittest.TestCase):
    def test_container_name_uses_names_or_short_id(self):
        self.assertEqual(container_name({"Names": ["/service"], "Id": "abcdef"}), "service")
        self.assertEqual(container_name({"Names": [], "Id": "abcdef1234567890"}), "abcdef123456")

    def test_read_timezone_valid_and_invalid(self):
        with mock.patch.dict(os.environ, {"TZ": "UTC"}):
            self.assertEqual(str(read_timezone()), "UTC")
        with mock.patch.dict(os.environ, {"TZ": "Invalid/Zone"}), quiet_logs():
            self.assertIs(read_timezone(), dt.timezone.utc)

    def test_read_positive_int_default_valid_and_invalid(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(read_positive_int("VALUE", 3), 3)
        with mock.patch.dict(os.environ, {"VALUE": "  "}, clear=True):
            self.assertEqual(read_positive_int("VALUE", 4), 4)
        with mock.patch.dict(os.environ, {"VALUE": "5"}, clear=True):
            self.assertEqual(read_positive_int("VALUE", 4), 5)
        with mock.patch.dict(os.environ, {"VALUE": "bad"}, clear=True):
            with self.assertRaises(SystemExit):
                read_positive_int("VALUE", 4)
        with mock.patch.dict(os.environ, {"VALUE": "0"}, clear=True):
            with self.assertRaises(SystemExit):
                read_positive_int("VALUE", 4)

    def test_read_non_negative_int_default_valid_and_invalid(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(read_non_negative_int("VALUE", 3), 3)
        with mock.patch.dict(os.environ, {"VALUE": "0"}, clear=True):
            self.assertEqual(read_non_negative_int("VALUE", 3), 0)
        with mock.patch.dict(os.environ, {"VALUE": "bad"}, clear=True):
            with self.assertRaises(SystemExit):
                read_non_negative_int("VALUE", 3)
        with mock.patch.dict(os.environ, {"VALUE": "-1"}, clear=True):
            with self.assertRaises(SystemExit):
                read_non_negative_int("VALUE", 3)

    def test_read_bool_default_valid_and_invalid(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(read_bool("VALUE", True))
        with mock.patch.dict(os.environ, {"VALUE": "off"}, clear=True):
            self.assertFalse(read_bool("VALUE", True))
        with mock.patch.dict(os.environ, {"VALUE": "YES"}, clear=True):
            self.assertTrue(read_bool("VALUE", False))
        with mock.patch.dict(os.environ, {"VALUE": "maybe"}, clear=True):
            with self.assertRaises(SystemExit):
                read_bool("VALUE", True)

    def test_main_wires_controller_and_signal_handler(self):
        handlers = {}
        created = {}

        class FakeController:
            def __init__(self, **kwargs):
                created["controller"] = self
                created["kwargs"] = kwargs
                self.run_called = False
                self.stop_called = False

            def run(self):
                self.run_called = True

            def stop(self):
                self.stop_called = True

        fake_docker = object()

        def fake_signal(signum, handler):
            handlers[signum] = handler

        env = {
            "DOCKER_SOCKET": "/custom.sock",
            "DISCOVERY_INTERVAL_SECONDS": "11",
            "DOCKER_TIMEOUT_SECONDS": "12",
            "JOB_TIMEOUT_SECONDS": "13",
            "MAX_CONCURRENT_JOBS": "14",
            "MAX_JITTER_SECONDS": "15",
            "OUTPUT_LIMIT_BYTES": "16",
            "LOG_JOB_OUTPUT": "false",
        }
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch.object(dc, "DockerClient", mock.Mock(return_value=fake_docker)) as docker_client,
            mock.patch.object(dc, "DockerCron", FakeController),
            mock.patch.object(dc, "read_timezone", mock.Mock(return_value=dt.timezone.utc)),
            mock.patch.object(dc.socket, "gethostname", mock.Mock(return_value="host-id")),
            mock.patch.object(dc.signal, "signal", fake_signal),
            quiet_logs(),
        ):
            self.assertEqual(dc.main(), 0)
            handlers[signal.SIGTERM](signal.SIGTERM, None)

        docker_client.assert_called_once_with("/custom.sock", 12)
        self.assertTrue(created["controller"].run_called)
        self.assertTrue(created["controller"].stop_called)
        self.assertEqual(created["kwargs"]["docker"], fake_docker)
        self.assertEqual(created["kwargs"]["discovery_interval_seconds"], 11)
        self.assertEqual(created["kwargs"]["self_container_id"], "host-id")
        self.assertEqual(created["kwargs"]["job_timeout_seconds"], 13)
        self.assertEqual(created["kwargs"]["max_concurrent_jobs"], 14)
        self.assertEqual(created["kwargs"]["max_jitter_seconds"], 15)
        self.assertEqual(created["kwargs"]["output_limit_bytes"], 16)
        self.assertFalse(created["kwargs"]["log_job_output"])

    def test_entrypoint_raises_system_exit_from_main_result(self):
        with mock.patch.object(dc, "main", mock.Mock(return_value=3)):
            with self.assertRaises(SystemExit) as raised:
                dc.entrypoint()
        self.assertEqual(raised.exception.code, 3)


if __name__ == "__main__":
    unittest.main()
