import "dotenv/config";

import express from "express";
import userRoutes from "./routes/users.js";

const app = express();

app.use(express.json());


/*
 * Health check
 */
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
  });
});


/*
 * User routes
 */
app.use("/users", userRoutes);


const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`API running on port ${PORT}`);
});