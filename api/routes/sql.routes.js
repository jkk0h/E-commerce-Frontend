import { Router } from "express";
import { pool } from "../db/pool.js";
import { ALLOW_SQL } from "../config/env.js";

const r = Router();

// DEV ONLY: guarded by env ALLOW_SQL=true
r.post("/query", async (req, res, next) => {
  try {
    if (!ALLOW_SQL) return res.status(403).json({ error: "Raw SQL endpoint disabled" });
    const { sql, params } = req.body || {};
    if (!sql || typeof sql !== "string") {
      return res.status(400).json({ error: "sql string required" });
    }
    const { rows } = await pool.query(sql, Array.isArray(params) ? params : []);
    res.json({ rows });
  } catch (e) { next(e); }
});

export default r;
