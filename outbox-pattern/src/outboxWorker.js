import "dotenv/config";
import pool from "../db/postgres.js";
import redis from "../cache/redis.js";



async function processOutbox() {

  const client = await pool.connect();

  try {

    /*
     * Start transaction.
     *
     * We lock rows using FOR UPDATE SKIP LOCKED.
     *
     * This means multiple workers can safely
     * process different events.
     */
    await client.query("BEGIN");

    const result = await client.query(`
      SELECT *
      FROM outbox_events
      WHERE processed_at IS NULL
      ORDER BY created_at
      FOR UPDATE SKIP LOCKED
      LIMIT 10
    `);


    for (const event of result.rows) {

      try {

        console.log(
          `Processing event ${event.id} (${event.event_type})`
        );

        /*
         * Increment attempt counter.
         */
        await client.query(
          `
          UPDATE outbox_events
          SET attempts = attempts + 1
          WHERE id = $1
          `,
          [event.id]
        );


        /*
         * Process event.
         */
        await handleEvent(event);


        /*
         * Mark event as processed.
         */
        await client.query(
          `
          UPDATE outbox_events
          SET processed_at = NOW(),
              last_error = NULL
          WHERE id = $1
          `,
          [event.id]
        );


        console.log(
          `Event ${event.id} processed successfully`
        );

      } catch (error) {

        console.error(
          `Failed processing event ${event.id}:`,
          error.message
        );


        /*
         * Don't mark it as processed.
         *
         * It will be retried next time.
         */
        await client.query(
          `
          UPDATE outbox_events
          SET last_error = $1
          WHERE id = $2
          `,
          [error.message, event.id]
        );
      }
    }


    await client.query("COMMIT");

  } catch (error) {

    await client.query("ROLLBACK");

    console.error(
      "Outbox worker error:",
      error
    );

  } finally {

    client.release();
  }
}


/*
 * Event dispatcher
 */
async function handleEvent(event) {

  switch (event.event_type) {

    case "USER_CREATED":
      await handleUserCreated(event);
      break;

    case "USER_UPDATED":
      await handleUserUpdated(event);
      break;

    default:
      throw new Error(
        `Unknown event type: ${event.event_type}`
      );
  }
}


/*
 * USER_CREATED
 */
async function handleUserCreated(event) {

  const user = event.payload;

  /*
   * Put user into Redis.
   */
  await redis.set(
    `user:${user.userId}`,
    JSON.stringify(user),
    "EX",
    60 * 10
  );

  console.log(
    `Cached user ${user.userId}`
  );
}


/*
 * USER_UPDATED
 */
async function handleUserUpdated(event) {

  const user = event.payload;

  /*
   * We can either update the cache...
   */

  await redis.set(
    `user:${user.userId}`,
    JSON.stringify(user),
    "EX",
    60 * 10
  );

  /*
   * OR invalidate it:
   *
   * await redis.del(`user:${user.userId}`);
   */

  console.log(
    `Updated cache for user ${user.userId}`
  );
}


/*
 * Run every second.
 */
setInterval(processOutbox, 1000);


/*
 * Also process immediately when worker starts.
 */
processOutbox();