import { Router } from "express";
import { pickProductSource } from "../services/sourcePicker.service.js";
import { getProducts, getProductById } from "../services/products.service.js";
import { toUiProduct } from "../utils/toUiProduct.js";

const r = Router();

// list
r.get("/", async (req, res, next) => {
  try {
    const { search = "", limit = "50", offset = "0" } = req.query;
    const src = await pickProductSource();
    if (src === "none") return res.json([]);
    const rows = await getProducts(String(search), Number(limit), Number(offset), src);
    res.json(rows.map(toUiProduct));
  } catch (e) { next(e); }
});

// detail
r.get("/:id", async (req, res, next) => {
  try {
    const src = await pickProductSource();
    if (src === "none") return res.status(404).json({ error: "Not found" });
    const row = await getProductById(String(req.params.id), src);
    if (!row) return res.status(404).json({ error: "Not found" });
    res.json(toUiProduct(row));
  } catch (e) { next(e); }
});

// (optional) simple create for admin demo
r.post("/", (req, res) => {
  const { sku, title, price } = req.body || {};
  if (!sku) return res.status(400).json({ error: "sku required (maps to product_id)" });
  res.status(201).json({
    id: sku, sku,
    title: title ?? sku,
    name: title ?? sku,
    price: price ?? null,
    order_count: 0
  });
});

export default r;
