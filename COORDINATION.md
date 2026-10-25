# Super T3 coordination (fireheartjerry/t3code)

## Purpose

This fork is Jerry's custom T3 Code ("super T3"). The Mac Codex and the Windows Codex edit it in parallel on separate branches and merge small PRs into one integration branch, `custom/main`. `main` mirrors nothing in particular; `upstream` = pingdotgg/t3code.

## Branches and ownership

| Branch        | Owner                           | Scope                                                                                                                                                                                                                                                                                    |
| ------------- | ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `custom/main` | integration (both, via PR only) | Base: upstream `7bafc40c` + our merged PRs                                                                                                                                                                                                                                               |
| `mac/server`  | MacBook Codex                   | `apps/server` orchestration features: checkout ownership + verification evidence (spec on the Mac: `~/Documents/Codex/2026-10-25/you-are-on-my-macbook-jerrys/outputs/2026-10-25-t3-verification-evidence-and-checkout-ownership-spec.md`), plus `packages/contracts` changes those need |
| `win/ui`      | Windows (FIREHPC) Codex         | `apps/web`, `apps/desktop` UI, the mint-dark theme (source: Mac `.../outputs/mint-dark.json`), installer and Windows-specific fixes                                                                                                                                                      |

Touching the other side's area: keep it minimal, say so in the PR body. Contract (`packages/contracts`) changes go in their own small PR first so the other side can rebase onto them.

## Rules

1. Small PRs into `custom/main` only. Never push directly to `custom/main`. Merge with `gh pr merge --squash`.
2. Before every PR: `git fetch origin && git rebase origin/custom/main`.
3. Upstream sync: once a week the Mac owner runs `git fetch upstream && git checkout custom/main && git merge upstream/main`, resolves conflicts, opens it as a PR (`sync/upstream-YYYY-MM-DD`). Never rebase `custom/main`.
4. Never run a dev server against the real `~/.t3/userdata` (Mac) or `%USERPROFILE%\.t3` (Windows). Use `vp run dev` inside the repo; it uses the repo's gitignored `.t3` (or pass an explicit `--home-dir`). Never set `VITE_HTTP_URL`/`VITE_WS_URL`.
5. Never kill processes by name or pattern (`pkill -f`, `taskkill /IM`, `Stop-Process -Name`). Only kill a PID you captured at spawn.
6. Line endings are LF. On Windows: `git config core.autocrlf input` and `git config core.eol lf` in this clone.
7. Checks before each PR (from AGENTS.md / package.json):
   - `vp i` (install; package manager is pnpm@12.14.0 via vite-plus, Node 26.12.1)
   - `vp run check:changed` (format, lint, knip, typecheck of affected packages, tests beside changes)
   - `vp test run <files>` while iterating
   - `vp run typecheck` (cached per package), `vp run lint`
   - Do not run repo-wide `vp check` / `vp run -r test` unless Jerry asks.
8. Conventional commit titles, e.g. `feat(server): checkout ownership records`.

## Testing without touching the installed T3

- Dev: `vp run dev` (server + web) in the repo; state lives in `<repo>/.t3`. For realistic data, stop the dev server and run `vp run migrate-dev-db` (read-only snapshot copy of the real DB).
- Desktop: `vp run dev:desktop`, or `vp run build:desktop` and run the built output from the repo. Do not install it over the real app, and do not overwrite `/Applications/T3 Code.app` or the Windows install.
- Different port from the live T3 (3773) is chosen by the dev runner; check its output.
- Shipping a custom build to the real app is a separate, explicit step Jerry approves.

## Active status

- `mac/server`: checkout admission and verification evidence implemented; contracts PR #3 precedes server integration. Focused checks: 634 tests pass. Native observation and descendant liveness remain conservative; no rollout or installation performed.
