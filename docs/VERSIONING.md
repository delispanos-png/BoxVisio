# Versioning

The app reports a version like **`2.7.109`**.

```
2  .  7  .  109
│     │     └── build — the commit count, increments by itself
│     └──────── minor — you bump this for a feature release
└────────────── major — you bump this for a big/breaking change
```

## Where each part comes from

| Part | Source | Who changes it |
| --- | --- | --- |
| `2.7` | the `VERSION` file at the repo root | you, by editing the file |
| `109` | commit count, stamped into `backend/app/build_info.json` | the `pre-commit` hook, automatically |

`build_info.json` is committed alongside the code, so every commit carries its
own version and any checkout can report what it is.

## Why it is a file and not a git command

The API container mounts the repo but **has no `git` binary**. Anything computed
by shelling out to git would silently fall back to a placeholder in production —
which is exactly what used to happen: the version was the short HEAD SHA
(`13064b1`), read from `.git/HEAD`, and it never moved because the work was
never committed.

## Setup (once per clone)

```bash
./scripts/install_git_hooks.sh
```

Without the hook the build number stops moving and the app keeps reporting the
last stamped build. The hook aborts the commit if stamping fails, so it cannot
silently drift.

## Doing a release

```bash
echo "2.8" > VERSION                       # feature release
python scripts/bump_build.py --current     # restamp now, or just commit
```

Editing `VERSION` alone does not change what the app reports until the next
stamp — `build_info.json` holds the fully rendered string. Commit, or run
`bump_build.py --current`.

## The `+dev` marker

Operators see a longer string; tenants never do.

| Where | Shows | Example |
| --- | --- | --- |
| Tenant sidebar, login page | clean version | `v2.7.109` |
| Admin panel, `GET /health` | version + provenance | `2.7.109+dev · 13064b1` |

`+dev` means **application files are newer than the last commit** — the running
code is not what is recorded in git. That is the normal state when editing the
live working tree, and it is the answer to "I changed things, why is the version
the same?": the version tracks commits, not saves.

It is detected by comparing file mtimes under `backend/app`, `scripts` and
`worker` against the mtime of `.git/index` (rewritten by `commit` and `git add`).
A heuristic, deliberately: a true dirty check needs the git binary. It is
re-evaluated at most every 30 seconds.

## A version change needs an API restart

`settings` is cached for the process lifetime, so the version is read once at
startup. Templates and static files reload live, but the reported version does
not — restart the API to publish a new one:

```bash
docker compose restart api
curl -sk https://bi.boxvisio.com/health | jq .
```
