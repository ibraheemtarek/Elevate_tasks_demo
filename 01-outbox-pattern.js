/**
 * OUTBOX PATTERN — guaranteeing DB write + Cache update happen reliably together
 * ------------------------------------------------------------------------------
 * Problem: if you write to the DB and then write to the cache as two separate
 * operations, there's a window where one succeeds and the other fails (dual-write
 * problem). The DB and cache drift apart, and there's no way to "roll back" a
 * cache write you already made, or retry a DB write you already committed.
 *
 * Fix: Never write to the cache directly from the request path. Instead, inside
 * the SAME database transaction as your business write, insert a row into an
 * "outbox" table describing the change that needs to propagate. Because it's the
 * same transaction, the business write and the outbox row are atomic — both
 * happen or neither does. A separate background process (the "relay") polls the
 * outbox table (or reads the DB's replication/WAL log via CDC, e.g. Debezium),
 * applies each pending event to the cache, and marks it processed — retrying
 * until it succeeds. This guarantees the cache is eventually updated, exactly
 * matching every committed DB write, without ever losing an update.
 *
 * In production: `db.accounts` / `db.outbox` below would be real Postgres/MySQL
 * tables, `db.transaction` would be a real SQL transaction (BEGIN/COMMIT), and
 * `cache` would be Redis. The relay would run as its own worker/service.
 */

// ---------- In-memory stand-in for a transactional relational DB ----------
class InMemoryDB {
  constructor() {
    this.accounts = new Map(); // id -> { id, balance }
    this.outbox = new Map();   // id -> { id, type, payload, status, retries, createdAt }
    this._outboxSeq = 1;
  }

  _snapshot() {
    return {
      accounts: new Map(Array.from(this.accounts, ([k, v]) => [k, { ...v }])),
      outbox: new Map(Array.from(this.outbox, ([k, v]) => [k, { ...v }])),
      seq: this._outboxSeq,
    };
  }

  _restore(snap) {
    this.accounts = snap.accounts;
    this.outbox = snap.outbox;
    this._outboxSeq = snap.seq;
  }

  // Mimics BEGIN ... COMMIT / ROLLBACK. If fn throws, all writes are undone.
  transaction(fn) {
    const snap = this._snapshot();
    try {
      return fn(this);
    } catch (err) {
      this._restore(snap);
      throw err;
    }
  }
}

// ---------- In-memory stand-in for Redis, with an injectable failure rate ----------
class FlakyCache {
  constructor(failureRate = 0) {
    this.store = new Map();
    this.failureRate = failureRate;
  }
  set(key, value) {
    if (Math.random() < this.failureRate) {
      throw new Error("simulated transient cache/network failure");
    }
    this.store.set(key, value);
  }
  get(key) {
    return this.store.get(key);
  }
}

// ---------- Business write: balance update + outbox row, ONE transaction ----------
function updateAccountBalance(db, accountId, newBalance, { simulateCrashBeforeOutbox = false } = {}) {
  return db.transaction((tx) => {
    const account = tx.accounts.get(accountId);
    tx.accounts.set(accountId, { ...account, balance: newBalance });

    if (simulateCrashBeforeOutbox) {
      // Something goes wrong before we can write the outbox row.
      throw new Error("simulated crash before outbox insert");
    }

    const id = tx._outboxSeq++;
    tx.outbox.set(id, {
      id,
      type: "ACCOUNT_BALANCE_UPDATED",
      payload: { accountId, balance: newBalance },
      status: "PENDING",
      retries: 0,
      createdAt: Date.now(),
    });
    return id;
  });
}

// ---------- The relay: polls outbox, applies to cache, retries on failure ----------
class OutboxRelay {
  constructor(db, cache, { maxRetries = 8 } = {}) {
    this.db = db;
    this.cache = cache;
    this.maxRetries = maxRetries;
  }

  pollOnce() {
    for (const event of this.db.outbox.values()) {
      if (event.status !== "PENDING") continue;
      try {
        this.cache.set(`account:${event.payload.accountId}`, event.payload.balance);
        event.status = "PROCESSED";
        console.log(`  [relay] ✓ outbox #${event.id} applied to cache`);
      } catch (err) {
        event.retries += 1;
        console.log(`  [relay] ✗ outbox #${event.id} failed (${err.message}), retry ${event.retries}`);
        if (event.retries >= this.maxRetries) {
          event.status = "FAILED";
          console.log(`  [relay] ! outbox #${event.id} giving up after ${this.maxRetries} retries`);
        }
      }
    }
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------- Demo ----------------------------
async function main() {
  const db = new InMemoryDB();
  db.accounts.set("acc-1", { id: "acc-1", balance: 100 });

  console.log("1) Normal write: business update + outbox row committed atomically");
  updateAccountBalance(db, "acc-1", 250);
  console.log("   DB balance:", db.accounts.get("acc-1").balance);
  console.log("   Outbox row:", db.outbox.get(1));

  console.log("\n2) Simulated crash BEFORE the outbox row is written -> whole transaction rolls back");
  try {
    updateAccountBalance(db, "acc-1", 9999, { simulateCrashBeforeOutbox: true });
  } catch (err) {
    console.log("   caught:", err.message);
  }
  console.log("   DB balance is still:", db.accounts.get("acc-1").balance, "(NOT 9999 — atomicity held)");

  console.log("\n3) Relay propagates the pending outbox event to a flaky cache, retrying until it lands");
  const cache = new FlakyCache(0.6); // fails 60% of the time, like a shaky network
  const relay = new OutboxRelay(db, cache);

  for (let round = 1; cache.get("account:acc-1") === undefined && round <= 10; round++) {
    console.log(`  -- relay poll round ${round}`);
    relay.pollOnce();
    await sleep(50);
  }

  console.log("\n4) Final state:");
  console.log("   DB balance:   ", db.accounts.get("acc-1").balance);
  console.log("   Cache value:  ", cache.get("account:acc-1"));
  console.log(
    "   In sync:",
    db.accounts.get("acc-1").balance === cache.get("account:acc-1")
  );
}

main();
