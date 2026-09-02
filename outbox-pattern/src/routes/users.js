import express from "express";

import { createUser, updateUser, getUserFromDatabase } from "../services/userService.js";
import redis from "../cache/redis.js";

const router = express.Router();


/*
 * GET USER
 *
 * Cache Aside Pattern
 */
router.get("/:id", async (req, res) => {
  try {
    const { id } = req.params;

    /*
     * 1. Try Redis first
     */
    const cachedUser = await redis.get(`user:${id}`);

    if (cachedUser) {
      console.log("CACHE HIT");

      return res.json({
        source: "cache",
        user: JSON.parse(cachedUser),
      });
    }

    console.log("CACHE MISS");

    /*
     * 2. Get from PostgreSQL
     */
    const user = await getUserFromDatabase(id);

    if (!user) {
      return res.status(404).json({
        error: "User not found",
      });
    }

    /*
     * 3. Put it in Redis
     */
    await redis.set(
      `user:${id}`,
      JSON.stringify(user),
      "EX",
      60 * 10
    );

    return res.json({
      source: "database",
      user,
    });

  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Internal server error",
    });
  }
});


/*
 * CREATE USER
 */
router.post("/", async (req, res) => {
  try {
    const { name, email } = req.body;

    if (!name || !email) {
      return res.status(400).json({
        error: "name and email are required",
      });
    }

    const user = await createUser({
      name,
      email,
    });

    res.status(201).json({
      user,
    });

  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Could not create user",
    });
  }
});


/*
 * UPDATE USER
 */
router.put("/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const { name, email } = req.body;

    if (!name || !email) {
      return res.status(400).json({
        error: "name and email are required",
      });
    }

    const user = await updateUser(id, {
      name,
      email,
    });

    res.json({
      user,
    });

  } catch (error) {

    if (error.message === "USER_NOT_FOUND") {
      return res.status(404).json({
        error: "User not found",
      });
    }

    console.error(error);

    res.status(500).json({
      error: "Could not update user",
    });
  }
});


export default router;