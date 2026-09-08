# Long-term decisions

Decisions that outlive the feature that introduced them. Each one constrains
later changes: revisiting one is a deliberate act, and the new decision is
written here before the code follows it.

Local decisions — how a function is structured, why a helper exists, the shape
of one test — are not recorded. They are read from `docker_cron.py` and
`tests/test_docker_cron.py`. The user-facing contract is
[`README.md`](../README.md); the conventions for working in this repository are
[`CLAUDE.md`](../CLAUDE.md).

## Standard library only

The runtime and the test tooling use nothing outside the Python standard
library.

Why: this is one infrastructure daemon holding root-equivalent access to a
Docker host. Every dependency is another supply chain to trust and another
upgrade to schedule, and the standard library already covers HTTP over a Unix
socket, threading, and test running.

Consequences: the Docker API client, the cron parser, the exec stream decoder
and the coverage gate are hand-written and stay in this repository. `pip` never
runs — not in the image, not in CI.

## The Docker Engine HTTP API over the mounted Unix socket

The controller speaks HTTP to `/var/run/docker.sock` itself, instead of shelling
out to `docker` or using a client SDK.

Why: the image stays a Python interpreter plus one file, with no CLI binary to
ship and no version coupling to a client library. Discovery, event streaming and
exec are all plain HTTP requests against the same socket.

Consequences: `UnixHTTPConnection` exists because `http.client` cannot dial a
Unix socket, and Docker's multiplexed exec frames are decoded by hand.

## Container labels are the entire job configuration

Jobs are declared only as `cron.<jobname>.*` labels on the containers that run
them. There is no config file, no CLI and no API for defining a job.

Why: the job definition belongs to the service it serves, so it is deployed,
versioned and removed together with that service's compose file or manifest. No
second place to keep in sync, and no state the controller has to persist.

Consequences: the controller is stateless and restart-safe — a rescan rebuilds
the whole job set. Changing a schedule means redeploying the target container.
A job cannot outlive its container, by design.

## One runtime file

The whole controller is `docker_cron.py`.

Why: at this size, one file is faster to read end to end than a package is to
navigate, and it keeps the image and the coverage gate trivial. The section
boundaries inside the file (cron parsing, Docker client, label parsing, exec
output, controller, configuration) carry the structure a package would.

Revisit when the file stops fitting in one reading, not before.

## A hand-written five-field cron parser

`CronExpression` implements `minute hour day-of-month month day-of-week` with
`*`, steps, lists, ranges, and month and weekday aliases.

Why: it follows from the standard-library-only decision. Five fields with
traditional semantics is what users expect from a container label; seconds,
`@reboot` and other extensions are deliberately absent.

The day-of-month / day-of-week rule follows classic cron: when both fields are
restricted, either match runs the job.

## Scheduling: one reservation per job, a global cap on execs

Every due job takes a reservation on its job key, then waits out its jitter,
then competes for one of `MAX_CONCURRENT_JOBS` execution slots. Reservation and
slot are separate resources, taken in that order.

Why: a job that overruns its period must never pile up on itself, an unbounded
number of parallel execs would overload the host, and a job that is only
sleeping through its jitter must not hold a slot away from a job that is ready
to run. The scheduler marks the occurrence as handled before it reserves, so a
job it cannot start is reported once and not reconsidered for that minute.

The order is visible in `DockerCron._run_job_thread`; the effects a user sees
are listed in README → Behavior.

## A timeout stops waiting, not the command

When a job exceeds its timeout, the controller stops reading the exec stream,
logs the timeout, and keeps the job reserved until Docker reports the exec has
finished or the container has stopped.

Why: the Engine API offers no way to cancel a running exec. Releasing the
reservation at the timeout would let the next occurrence start on top of a
command that is still running — exactly the pile-up the reservation exists to
prevent. Hard cancellation is the command's own responsibility.

## Shell first, direct exec as a fallback

Commands run as `/bin/sh -lc "<command>"`. If the image has no shell, the
controller retries once with the command parsed by shell-like quoting rules.

Why: a label is written as a shell one-liner and usually needs a shell — pipes,
globs, environment from the login profile. Distroless and scratch images have no
shell at all, and failing them for that alone would be a poor trade. The
fallback is a retry rather than a mode, so nothing has to be configured.

## Failures are logged, and the controller keeps running

A failing scan, a dropped event stream or a failing job is logged and, where
relevant, triggers a rescan. Only invalid configuration at startup exits.

Why: a cron controller that dies on a transient Docker error takes every job
with it. A container that disappeared, a broken label, an exec that failed — all
of those are ordinary events in a live host and must not end the process.

## One-line JSON logs on stdout are the only observability channel

Every event is a single JSON object on stdout. There is no metrics endpoint, no
status file and no notification integration.

Why: `docker logs` and any log shipper already handle stdout, and structured
lines survive collection without a parser. Reporting integrations are what
[Ofelia](https://github.com/mcuadros/ofelia) is for — see README → Alternatives.

Consequences: the log field names are part of the public contract; renaming one
breaks whatever is parsing them.

## The tests are the specification, at 100% line coverage

`tests/test_docker_cron.py` covers every line of `docker_cron.py`, enforced by
`tools/check_coverage.py`. Lines that cannot be reached from a test are marked
`# pragma: no cover` and are the exception, not the escape hatch.

Why: the code is the source of truth for how the controller behaves, so it has
to be true — and a full gate is the only threshold that needs no argument about
what is worth testing. It is also what makes the documentation model work: once
a feature ships, its tests describe it precisely enough that the plan can be
deleted.

Concurrency and socket behavior are tested against real threads and real
sockets, not mocks, because the bugs worth catching there live in the timing.

## The trust model: labels are a control plane

The controller does not sandbox, filter or authorize anything. Any actor that
can set a `cron.*` label can make it run commands, and the mounted socket gives
it root-equivalent power over the host.

Why: any check the controller could add would be enforced by a process an
attacker can already instruct through the same socket. The honest boundary is
the host, so the risk is stated to the user in README → Security instead of
being simulated in code.

## Release images are built from GitHub releases

Publishing a GitHub release builds a multi-arch (`linux/amd64`, `linux/arm64`)
image to GHCR, tagged with the released version as tagged (`v1.2` and `v1.2.3`
both publish, as `1.2` and `1.2.3`) and with `latest`. The coverage gate runs
first in the same workflow.

Why: the release is the deliberate act; a push to `main` is not. Nothing is
published that has not passed the gate.
