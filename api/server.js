import express from "express";
import cors from "cors";
import { connectAll } from "./db.js";

const app = express();
app.use(express.json());

// CORS: allow your frontend on Railway. Add GitHub Pages if you use it.
app.use(cors({ origin: [/\.up\.railway\.app$/] }));

const { pgPool, mongoDb } = await connectAll();

// --- API routes ---
// Add near your other routes
app.get("/health", (_req, res) => res.json({ ok: true }));

app.get("/diagnostics", async (_req, res) => {
  try {
    // Postgres ping
    const { rows: pgRows } = await pgPool.query("select 1 as ok");
    // Mongo ping + sample count
    const mongoPing = await mongoDb.command({ ping: 1 });
    const productsCount = await mongoDb.collection("products").countDocuments().catch(() => -1);

    res.json({
      api: "ok",
      postgres: pgRows[0]?.ok === 1 ? "ok" : "fail",
      mongo: mongoPing?.ok === 1 ? "ok" : "fail",
      productsCount
    });
  } catch (e) {
    res.status(500).json({ api: "fail", error: e.message });
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log("API listening on", port));
