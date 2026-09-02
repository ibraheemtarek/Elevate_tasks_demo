/**
 * TOKEN BUCKET RATE LIMITING
 * ---------------------------
 * A bucket holds up to `capacity` tokens. Tokens refill continuously at
 * `refillRate` tokens/second. Every request must "spend" 1 token (or N for
 * weighted requests) to proceed; if the bucket is empty, the request is
 * rejected/throttled. Because tokens accumulate while idle (up to the cap),
 * the algorithm allows short bursts up to `capacity` while still enforcing
 * the long-run average rate of `refillRate` — unlike a naive fixed-window
 * counter, it doesn't have a hard reset boundary that lets two full bursts
 * land back-to-back.
 *
 * Implementation trick: you don't need a timer ticking every millisecond.
 * Just record the last refill timestamp, and lazily compute how many tokens
 * should have accumulated the next time someone tries to consume.
 */

class TokenBucket {
  constructor({ capacity, refillRatePerSec, now = () => Date.now() }) {
    this.capacity = capacity;
    this.refillRate = refillRatePerSec;
    this.tokens = capacity; // start full
    this.now = now;
    this.lastRefill = this.now();
  }

  _refill() {
    const now = this.now();
    const elapsedSec = (now - this.lastRefill) / 1000;
    if (elapsedSec > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + elapsedSec * this.refillRate);
      this.lastRefill = now;
    }
  }

  tryConsume(n = 1) {
    this._refill();
    if (this.tokens >= n) {
      this.tokens -= n;
      return true;
    }
    return false;
  }
}

// ---------- Express-style middleware, one bucket per client key ----------
function tokenBucketMiddleware({ capacity, refillRatePerSec, keyFn = (req) => req.ip }) {
  const buckets = new Map();
  return function rateLimit(req, res, next) {
    const key = keyFn(req);
    if (!buckets.has(key)) {
      buckets.set(key, new TokenBucket({ capacity, refillRatePerSec }));
    }
    if (buckets.get(key).tryConsume(1)) {
      next();
    } else {
      res.status(429).json({ error: "Too Many Requests" });
    }
  };
}
// Usage: app.use(tokenBucketMiddleware({ capacity: 20, refillRatePerSec: 5 }));

/**
 * DISTRIBUTED version: an in-process bucket only limits traffic hitting ONE
 * server. Behind a load balancer with multiple instances, you need the bucket
 * state to live somewhere shared — Redis — and the refill+check+consume must
 * be atomic across concurrent requests from different servers. That's another
 * job for a Lua script (EVAL), run via ioredis as:
 *   redis.eval(TOKEN_BUCKET_LUA, 1, `bucket:${clientId}`, capacity, refillRatePerSec, Date.now(), 1)
 *
 *   -- KEYS[1] = bucket key
 *   -- ARGV[1]=capacity ARGV[2]=refill_rate_per_sec ARGV[3]=now_ms ARGV[4]=requested
 *   local capacity = tonumber(ARGV[1])
 *   local refill_rate = tonumber(ARGV[2])
 *   local now = tonumber(ARGV[3])
 *   local requested = tonumber(ARGV[4])
 *
 *   local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
 *   local tokens = tonumber(data[1]) or capacity
 *   local ts = tonumber(data[2]) or now
 *
 *   local elapsed = math.max(0, now - ts) / 1000.0
 *   tokens = math.min(capacity, tokens + elapsed * refill_rate)
 *
 *   local allowed = 0
 *   if tokens >= requested then
 *     tokens = tokens - requested
 *     allowed = 1
 *   end
 *
 *   redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
 *   redis.call('EXPIRE', KEYS[1], 3600)
 *   return allowed
 *
 * Same lazy-refill math as the JS class above — just executed atomically on
 * the Redis server so every app instance shares one consistent bucket.
 */

// ---------------------------- Demo (virtual clock, fully deterministic) ----------------------------
function main() {
  let virtualNow = 0;
  const bucket = new TokenBucket({
    capacity: 10,
    refillRatePerSec: 5,
    now: () => virtualNow,
  });

  console.log("Bucket: capacity=10, refill=5 tokens/sec\n");

  console.log("Burst of 15 requests at t=0ms (bucket starts full at 10 tokens):");
  let allowed = 0, rejected = 0;
  for (let i = 1; i <= 15; i++) {
    const ok = bucket.tryConsume(1);
    ok ? allowed++ : rejected++;
    console.log(`  request #${String(i).padStart(2)} -> ${ok ? "ALLOWED" : "REJECTED (429)"}`);
  }
  console.log(`  => ${allowed} allowed, ${rejected} rejected out of 15\n`);

  console.log("Advancing virtual clock by 1000ms (bucket should refill ~5 tokens)...");
  virtualNow += 1000;

  console.log("Next burst of 8 requests:");
  allowed = 0; rejected = 0;
  for (let i = 1; i <= 8; i++) {
    const ok = bucket.tryConsume(1);
    ok ? allowed++ : rejected++;
    console.log(`  request #${i} -> ${ok ? "ALLOWED" : "REJECTED (429)"}`);
  }
  console.log(`  => ${allowed} allowed, ${rejected} rejected out of 8`);
  console.log(
    "\nThis shows the core property: bursts up to capacity are allowed instantly,",
    "\nbut sustained throughput is capped at the refill rate over time."
  );
}

main();
