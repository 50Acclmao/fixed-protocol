# arras-headless improvements

## 1. Scale the farm to 1500 bots (`server.js`)
- New global farm cap `MAX_BOTS_GLOBAL` (default **1500**, override via `ARRAS_MAX_BOTS`).
  - `spawnBotNow()` refuses to spawn past the cap, so an `#F` flood request breaks cleanly.
  - Live `activeBotCount` is tracked on every spawn/death and **recomputed** after nukes and
    disconnects so it can never drift and block new spawns.
- Worker / proxy / pool limits are now env-tunable:
  - `ARRAS_WORKER_MEMORY_MB` (96) — 60 workers for 1500 bots is realistic
  - `ARRAS_BOTS_PER_WORKER` (25), `ARRAS_PREWARM_POOL` (16)
  - `ARRAS_MAX_PROXIES` (6000), `ARRAS_MAX_WORKERS` (128)
- Proxy pool stays healthy under load:
  - fetch/rank are concurrency-guarded (no overlapping runs)
  - the pool auto-refreshes every `ARRAS_PROXY_REFRESH_MS` (3 min) and a failed/empty fetch
    never wipes the live pool
  - the ranked (best) queue is rebuilt every cycle so defenders can't starve it via `shift()`
  - a drained session queue auto-refills from the live pool instead of killing the stagger
- Graceful `SIGINT`/`SIGTERM` shutdown: terminates all workers, kills protocol children,
  clears timers, closes the server.
- 30s liveness line: `[stats] bots=…/1500 spawned=… workers=… proxies=…/6000 ranked=…`.

## 2. Wavy movement that still reaches the position (`index.js`)
- `pathfind()` now walks with a **sine-wave sway** around the true heading to the target —
  a visible snake / weaving motion instead of a straight line.
- Guaranteed convergence, no "impossible to reach":
  - sway is clamped to ±0.45 rad (~26°), far below the 90° that would cancel forward progress
  - sway **fades to zero** once the bot is within 90 px (straight-line finish)
  - the bot **stops** once within 10 px of the target instead of orbiting
  - each bot gets a random phase offset, so 1500 bots don't weave in lockstep
- Tunable per farm via `wavy`, `wavyAmp`, `wavyFreq` on the shared target (defaults on).
- `updateAllTargets()` ignores `undefined` keys so controllers that don't send `wavy` can't
  accidentally turn it off.

## 3. Stability / leaks
- `currentBotInterfaces` no longer grows forever: destroyed bots are removed by id, and stale
  interfaces with the same id are dropped before pushing a reconnect respawn.
- `singInterval` is cleared on destroy.

## 4. Package hygiene (`package.json`)
- Added `name`/`version`/`description`/`engines` and a `check` script (`node --check`).
- Kept `wss@^3.3.4` — it is required for the Codespaces connection flow (same as the
  root `package.json`/lockfile), so it was **not** removed.