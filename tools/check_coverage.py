#!/usr/bin/env python3
"""Run the unit tests and enforce 100% line coverage for docker_cron.py.

This intentionally uses Python's stdlib trace module so the project keeps its
runtime and test tooling dependency-free.
"""

from __future__ import annotations

import contextlib
import io
import pathlib
import sys
import trace
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
TARGET = ROOT / "docker_cron.py"
sys.path.insert(0, str(ROOT))


def ignored_lines(path: pathlib.Path) -> set[int]:
    ignored: set[int] = set()
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if "pragma: no cover" in line:
            ignored.add(line_number)
    return ignored


def run_tests() -> unittest.result.TestResult:
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
    runner = unittest.TextTestRunner(verbosity=1)
    return runner.run(suite)


def main() -> int:
    tracer = trace.Trace(count=True, trace=False)
    with contextlib.redirect_stdout(io.StringIO()):
        result = tracer.runfunc(run_tests)
    if not result.wasSuccessful():
        return 1

    line_count = len(TARGET.read_text(encoding="utf-8").splitlines())
    executable = {
        line_number
        for line_number in trace._find_executable_linenos(str(TARGET))
        if 1 <= line_number <= line_count
    } - ignored_lines(TARGET)
    counts = tracer.results().counts
    executed = {
        line_number
        for (filename, line_number), count in counts.items()
        if pathlib.Path(filename).resolve() == TARGET and count > 0
    }
    missing = sorted(executable - executed)

    if missing:
        print(f"docker_cron.py coverage: {len(executable) - len(missing)}/{len(executable)} lines")
        print("Missing lines:", ", ".join(str(line) for line in missing))
        return 1

    print(f"docker_cron.py coverage: {len(executable)}/{len(executable)} lines (100%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
