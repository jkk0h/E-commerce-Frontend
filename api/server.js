import express from "express";
import cors from "cors";
import { connectAll } from "./db.js";

const app = express();
app.use(express.json());
app.use(cors({ origin: [/\.up\.railway\.app$/] }));

const { pgPool, mongoDb } = await connectAll();

app.get("/health", (_req, res) => res.json({ ok: true }));

app.get("/diagnostics", async (_req, res) => {
  try {
    const { rows } = await pgPool.query("select 1 as ok");
    let mongo = "skipped", productsCount = -1;
    if (mongoDb) {
      const ping = await mongoDb.command({ ping: 1 });
      mongo = ping?.ok === 1 ? "ok" : "fail";
      productsCount = await mongoDb.collection("products").countDocuments().catch(() => -1);
    }
    res.json({ api: "ok", postgres: rows[0]?.ok === 1 ? "ok" : "fail", mongo, productsCount });
  } catch (e) {
    res.status(500).json({ api: "fail", error: e.message });
  }
});

app.get("/api/products/:id", async (req, res) => {
    try {
        const productId = req.params.id;
        if (!mongoDb) {
            return res.status(503).json({ error: "MongoDB connection not available." });
        }

        // Fetch the product using the ID. Adjust 'product_id' if your collection uses a different key.
        const product = await mongoDb.collection("products").findOne({ product_id: productId });
        
        if (!product) {
            return res.status(404).json({ error: "Product not found" });
        }

        res.json(product);
    } catch (e) {
        console.error("Error fetching single product:", e);
        res.status(500).json({ error: "Failed to retrieve product details." });
    }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log("API listening on", port));
