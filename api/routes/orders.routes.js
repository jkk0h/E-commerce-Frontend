import { Router } from "express";
import { createOrder } from "../services/orders.service.js";

const r = Router();

r.post("/", async (req, res, next) => {
  try {
    const payload = req.body || {};
    const result = await createOrder(payload);
    res.status(201).json(result);
  } catch (e) { next(e); }
});

export default r;
