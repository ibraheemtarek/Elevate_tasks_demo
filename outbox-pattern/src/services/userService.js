import crypto from "crypto";
import pool from "../db/postgres.js";

async function createUser({ name, email }) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    /*
     * 1. Create user
     */
    const userResult = await client.query(
      `
      INSERT INTO users (name, email)
      VALUES ($1, $2)
      RETURNING id, name, email, created_at
      `,
      [name, email]
    );

    const user = userResult.rows[0];

    /*
     * 2. Create OUTBOX EVENT
     *
     * This is part of the SAME transaction.
     */
    await client.query(
      `
      INSERT INTO outbox_events (
        id,
        event_type,
        aggregate_type,
        aggregate_id,
        payload
      )
      VALUES ($1, $2, $3, $4, $5)
      `,
      [
        crypto.randomUUID(),
        "USER_CREATED",
        "User",
        user.id,
        JSON.stringify({
          userId: user.id,
          name: user.name,
          email: user.email,
        }),
      ]
    );

    /*
     * 3. Commit
     *
     * Both:
     *
     * users INSERT
     * +
     * outbox INSERT
     *
     * commit together.
     */
    await client.query("COMMIT");

    return user;

  } catch (error) {

    /*
     * If anything fails:
     *
     * users INSERT is rolled back
     * outbox INSERT is rolled back
     */
    await client.query("ROLLBACK");

    throw error;

  } finally {
    client.release();
  }
}


async function updateUser(id, { name, email }) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    /*
     * 1. Update user
     */
    const result = await client.query(
      `
      UPDATE users
      SET
        name = $1,
        email = $2,
        updated_at = NOW()
      WHERE id = $3
      RETURNING id, name, email, updated_at
      `,
      [name, email, id]
    );

    if (result.rowCount === 0) {
      throw new Error("USER_NOT_FOUND");
    }

    const user = result.rows[0];

    /*
     * 2. Insert outbox event
     */
    await client.query(
      `
      INSERT INTO outbox_events (
        id,
        event_type,
        aggregate_type,
        aggregate_id,
        payload
      )
      VALUES ($1, $2, $3, $4, $5)
      `,
      [
        crypto.randomUUID(),
        "USER_UPDATED",
        "User",
        user.id,
        JSON.stringify({
          userId: user.id,
          name: user.name,
          email: user.email,
        }),
      ]
    );

    /*
     * 3. Commit both operations
     */
    await client.query("COMMIT");

    return user;

  } catch (error) {

    await client.query("ROLLBACK");

    throw error;

  } finally {
    client.release();
  }
}


async function getUserFromDatabase(id) {
  const result = await pool.query(
    `
    SELECT id, name, email, created_at, updated_at
    FROM users
    WHERE id = $1
    `,
    [id]
  );

  return result.rows[0] || null;
}


export default {
  createUser,
  updateUser,
  getUserFromDatabase,
};