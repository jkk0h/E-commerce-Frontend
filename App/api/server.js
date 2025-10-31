import express from "express";
import cors from "cors";
import { connectAll } from "./db.js";

const app = express();
app.use(express.json());

// CORS: allow your frontend on Railway. Add GitHub Pages if you use it.
app.use(cors({ origin: [/\.up\.railway\.app$/] }));

const { pgPool, mongoDb } = await connectAll();

// --- API routes ---
// Simple health
app.get("/health", (_req, res) => res.json({ ok: true }));

// Products (from Mongo)
app.get("/api/products", async (_req, res) => {
  const list = await mongoDb.collection("products").find({ active: true }).limit(50).toArray();
  res.json(list);
});

// Monthly sales (from Postgres)
app.get("/api/sales-monthly", async (_req, res) => {
  const { rows } = await pgPool.query(`
    SELECT d.year_no, d.month_no, SUM(s.gross_amt) AS gross
    FROM sales s
    JOIN dates d ON s.date_id = d.date_id
    GROUP BY 1,2
    ORDER BY 1,2
  `);
  res.json(rows);
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log("API listening on", port));
