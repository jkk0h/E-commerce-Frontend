import { Router } from "express";
const r = Router();

r.get("/", (_req, res) => {
  res.json({ ok: true, service: "api", ts: new Date().toISOString() });
});

export default r;
