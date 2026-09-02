/**
 * VERSIONING + LUA SCRIPTS — fixing race conditions in write-behind caching
 * --------------------------------------------------------------------------
 * Write-behind (write-back): the app writes to the CACHE first (fast, synchronous)
 * and returns immediately. A background "flusher" later persists that change to
 * the DB, usually batched. This is fast, but introduces a real problem:
 *
 *   Because the flush to the DB (and even updates to the cache itself) happen
 *   asynchronously, on different workers, with retries and network jitter,
 *   updates can arrive OUT OF ORDER. If update A (older) is generated first but
 *   delayed, and update B (newer) is generated after but arrives first, a naive
 *   "just overwrite" flusher will apply B, then A — leaving the newer value B
 *   clobbered by the stale value A.
 *
 * VERSIONING fixes "which value is actually newer": every write carries a
 * monotonically increasing version (or timestamp/logical clock). A write is only
 * applied if its version is greater than what's currently stored — this makes
 * "last write wins" into "highest version wins", which is safe under reordering.
 *
 * LUA SCRIPTS fix "how do we check-and-set without a race": Redis runs a single
 * command atomically, but "GET version, compare in app code, then SET" is THREE
 * round trips — another client can sneak a write in between your GET and your
 * SET. A Lua script sent to Redis via EVAL runs entirely on the server as ONE
 * atomic step: Redis is single-threaded, so no other command can interleave
 * inside the script. That gives you a true compare-and-set with no race window.
 *
 * The real Redis Lua script equivalent of `compareAndSet` below (called via
 * ioredis as `redis.eval(script, 1, key, value, version)`):
 *
 *   -- KEYS[1] = cache key, ARGV[1] = new value, ARGV[2] = new version
 *   local current = redis.call('HGET', KEYS[1], 'version')
 *   if (not current) or (tonumber(ARGV[2]) > tonumber(current)) then
 *     redis.call('HSET', KEYS[1], 'value', ARGV[1], 'version', ARGV[2])
 *     return 1
 *   else
 *     return 0
 *   end
 *
 * Below, plain synchronous JS functions stand in for that Lua script — Node.js
 * is single-threaded too, so a synchronous function body is just as immune to
 * interleaving as a Redis Lua script is, which is exactly the property we want
 * to demonstrate.
 */

class VersionedStore {
  constructor(name) {
    this.name = name;
    this.store = new Map(); // key -> { value, version }
  }

  // Atomic compare-and-set by version (stands in for the Lua EVAL above)
  compareAndSet(key, value, version) {
    const current = this.store.get(key);
    if (!current || version > current.version) {
      this.store.set(key, { value, version });
      return { applied: true };
    }
    return {
      applied: false,
      reason: `incoming v${version} <= stored v${current.version}`,
    };
  }

  get(key) {
    return this.store.get(key);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// The cache-write path the app calls synchronously on every request
function writeThroughCache(redis, flushQueue, key, value, version) {
  const result = redis.compareAndSet(key, value, version);
  if (result.applied) {
    console.log(`  [cache] APPLIED  ${key} = "${value}" (v${version})`);
    flushQueue.push({ key, value, version });
  } else {
    console.log(`  [cache] REJECTED ${key} = "${value}" (v${version}) — ${result.reason}`);
  }
  return result;
}

// Background flusher: periodically drains queued writes into the DB.
// It ALSO uses compareAndSet, because flush jobs can themselves be retried or
// reordered (e.g. a slow flush worker, a retried job after a DB timeout).
async function runFlusher(db, flushQueue, { batches = 3, intervalMs = 80 } = {}) {
  for (let i = 0; i < batches; i++) {
    await sleep(intervalMs);
    const batch = flushQueue.splice(0, flushQueue.length);
    for (const job of batch) {
      const res = db.compareAndSet(job.key, job.value, job.version);
      console.log(
        `  [flusher] ${res.applied ? "wrote" : "skipped stale"} ${job.key}=${job.value} (v${job.version}) to DB`
      );
    }
  }
}

async function main() {
  const redis = new VersionedStore("redis");
  const db = new VersionedStore("db");
  const flushQueue = [];

  console.log("Scenario: two price updates for the same product race each other.");
  console.log("Update A (v2, price=19.99) is generated FIRST but its message is delayed.");
  console.log("Update B (v3, price=17.49) is generated SECOND but arrives at the cache FIRST.\n");

  const key = "product:42:price";

  // B (newer, v3) arrives quickly
  const updateB = sleep(20).then(() => writeThroughCache(redis, flushQueue, key, "17.49", 3));
  // A (older, v2) arrives late, simulating network delay / queue redelivery
  const updateA = sleep(150).then(() => writeThroughCache(redis, flushQueue, key, "19.99", 2));

  const flusherDone = runFlusher(db, flushQueue, { batches: 4, intervalMs: 60 });

  await Promise.all([updateA, updateB, flusherDone]);

  console.log("\nFinal state:");
  console.log("  redis:", redis.get(key));
  console.log("  db:   ", db.get(key));
  console.log(
    "\nWithout versioning, the late-arriving stale update (v2) would have overwritten",
    "\nthe newer value (v3). With compareAndSet-by-version, the stale write is rejected",
    "\nand both cache and DB converge on the newest value — regardless of arrival order."
  );
}

main();
