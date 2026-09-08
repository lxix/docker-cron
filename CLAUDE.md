# Working in this repository

`docker-cron` runs cron-like jobs inside already running containers, driven by
container labels. [`README.md`](README.md) says what it does; this file says how
we work on it.

## Documentation model

The code is the software manifestation of the documentation. Intent lives in
Markdown, behavior lives in code, and neither repeats the other.

| Question | Answered by |
| --- | --- |
| What does it do, how do I run it? | [`README.md`](README.md) |
| Why is it built this way? What must not change casually? | [`docs/decisions.md`](docs/decisions.md) |
| How does this particular thing work? | `docker_cron.py`, `tests/test_docker_cron.py` |
| What is being built right now? | `docs/plans/<name>.md` |

Rules:

- **One home per fact.** Before writing a sentence, find where that knowledge
  already lives and edit it there. A link is not duplication; a restatement is.
- **Self-documenting code.** Names, structure and control flow do the
  explaining. Code carries no decisions, no rationale and no narration of what
  the next line does.
- **Comment only the genuinely surprising** — a platform quirk, a protocol edge
  case, a workaround for someone else's bug: something that cannot be made
  obvious by naming or structure, and that a reader would otherwise "fix". Write
  it where it applies, once.
- **A shipped plan is deleted.** The moment a documented MVP, spike or feature
  is in the code, the code and its tests are the truth. Move anything long-term
  out of the plan into `docs/decisions.md`, then delete the plan file.
- **Record long-term decisions only.** A decision that outlives its feature and
  constrains later work goes into `docs/decisions.md`. A decision local to one
  feature is not written down anywhere — it is read from the code.
- **Behavior changes update `README.md` in the same change.**
- **Documentation is written in English**, whatever language the conversation
  uses.

## Repository map

```text
docker_cron.py           the controller: cron parsing, Docker client, scheduler
tests/                   the executable specification
tools/check_coverage.py  test runner and coverage gate
examples/compose.yml     a runnable demo: the controller plus two labeled services
Dockerfile               the published image
.github/workflows/       release image build
docs/plans/              work in progress; created on demand, deleted when it ships
```

## Development

```bash
python3 tools/check_coverage.py
```

Runs the unit tests and fails below 100% line coverage of `docker_cron.py`. It
must be green before every commit — the same gate runs before an image is
published.

Read [`docs/decisions.md`](docs/decisions.md) before changing the design: it
records what has already been decided, and why.
