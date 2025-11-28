// server-mongo.js  (ESM)
import express from "express";
import cors from "cors";
import { connectAll } from "./db.js";
import crypto from "crypto";

const PORT = process.env.MONGO_PORT || 3002;

const app = express();
app.use(cors());
app.use(express.json());

// connect to Postgres + Mongo using shared helper
const { pgPool, mongoDb } = await connectAll();
if (!mongoDb) {
    console.error("❌ Mongo is not configured (MONGODB_URI / MONGO_PUBLIC_URL missing).");
    process.exit(1);
}

// simple helper to measure DB time in ms (like your SQL server)
async function timeDb(fn) {
    const t0 = process.hrtime.bigint();
    const result = await fn();
    const t1 = process.hrtime.bigint();
    const dbMs = Number(t1 - t0) / 1e6;
    return { result, dbMs };
}

// ---------------- health ----------------
app.get("/api/health", async (_req, res) => {
    try {
        // just ping Mongo (you *can* also ping Postgres if you want)
        await mongoDb.command({ ping: 1 });
        res.json({ ok: true, engine: "mongo" });
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});

// ---------------- products (Mongo) ----------------
// Uses `products` collection seeded by admin + tools
app.get("/api/products", async (req, res) => {
    try {
        const search = String(req.query.search || "").trim();
        const filter = { active: true };

        if (search) {
            filter.$or = [
                { product_id: { $regex: search, $options: "i" } },
                { title: { $regex: search, $options: "i" } },
            ];
        }

        const { result, dbMs } = await timeDb(() =>
            mongoDb.collection("orders").find(filter).toArray()
        );

        // shape products like your UI expects
        const products = result.map((doc) => ({
            id: doc.product_id,
            product_id: doc.product_id,
            title: doc.title || doc.product_id,
            name: doc.title || doc.product_id,
            price: doc.price != null ? Number(doc.price) : null,
            order_count: Number(doc.order_count || 0),
        }));

        res.json(products); // or { results: products, dbMs } if you prefer
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});


// single product detail
app.get("/api/products/:id", async (req, res) => {
    try {
        const id = String(req.params.id);
        const { result } = await timeDb(() =>
            mongoDb.collection("orders").findOne({ product_id: id })
        );

        if (!result) return res.status(404).json({ error: "Not found" });

        const product = {
            id: result.product_id,
            product_id: result.product_id,
            title: result.title || result.product_id,
            name: result.title || result.product_id,
            price: result.price != null ? Number(result.price) : null,
            order_count: Number(result.order_count || 0),
        };

        res.json(product);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});


// admin "create product" in Mongo mode: also records an admin order in `orders`
app.post("/api/products", async (req, res) => {
    try {
        const { product_id, title, price, quantity } = req.body || {};

        if (!product_id) {
            return res.status(400).json({ error: "product_id required" });
        }

        const unitPrice = price != null ? Number(price) : 0;
        if (!Number.isFinite(unitPrice) || unitPrice < 0) {
            return res
                .status(400)
                .json({ error: "price must be a non-negative number" });
        }

        const qty = Math.max(1, parseInt(quantity ?? 1, 10) || 1);

        // ----- build an "admin order" for the orders collection -----
        const orderId = crypto.randomBytes(16).toString("hex");
        const customerId = "admin_seed"; // marker so you know it came from admin
        const DEFAULT_FREIGHT_PER_ITEM = 0;

        const docs = [];
        let orderItemId = 1;
        let total = 0;

        for (let i = 0; i < qty; i++) {
            docs.push({
                order_id: orderId,
                order_item_id: orderItemId,
                product_id,
                price: unitPrice,
                freight_value: DEFAULT_FREIGHT_PER_ITEM,
                customer_id: customerId,
                source: "admin",          // 👈 extra flag so you can distinguish later
                created_at: new Date(),
            });
            orderItemId++;
            total += unitPrice + DEFAULT_FREIGHT_PER_ITEM;
        }

        // time both: insert into orders AND update products summary
        const { dbMs } = await timeDb(async () => {
            // 1) write admin transaction history into `orders`
            await mongoDb.collection("orders").insertMany(docs);

            // 2) maintain summary row in `products`
            return await mongoDb.collection("products").updateOne(
                { product_id },
                {
                    $setOnInsert: {
                        product_id,
                        active: true,
                        created_at: new Date(),
                    },
                    $set: {
                        title: title || product_id,
                        price: unitPrice,
                    },
                    $inc: { order_count: qty },
                },
                { upsert: true }
            );
        });

        const productForUi = {
            id: product_id,
            product_id,
            title: title || product_id,
            name: title || product_id,
            price: unitPrice,
            order_count: qty, // delta for this admin action
        };

        res.status(201).json({
            product: productForUi,
            orderId,
            total,
            mongoDbMs: dbMs,
        });
    } catch (e) {
        console.error("Mongo admin seed error:", e);
        res.status(400).json({ error: e.message });
    }
});




// ---------------- checkout -> Mongo orders ----------------
// Insert one document per order item, similar shape to the old mirrored design.
app.post("/api/orders", async (req, res) => {
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) {
        return res.status(400).json({ error: "items required" });
    }

    // Generate a 32-char hex order ID (like the SQL service)
    const orderId = crypto.randomBytes(16).toString("hex");

    const rawCustomerId =
        req.body?.customer_id && String(req.body.customer_id).trim().length
            ? String(req.body.customer_id).trim()
            : "guest";

    const customerId = rawCustomerId;
    const DEFAULT_FREIGHT_PER_ITEM = 0;

    const docs = [];
    let orderItemId = 1;
    let total = 0;

    // Flatten items into one doc per unit:
    for (const item of items) {
        const productIdRaw = item?.id != null ? String(item.id).trim() : "";
        const qty = Math.max(1, parseInt(item?.quantity, 10) || 1);
        const price = item?.price != null ? Number(item.price) : 0;

        if (!productIdRaw) continue;
        if (!Number.isFinite(price)) continue;

        const productId = productIdRaw;

        for (let i = 0; i < qty; i++) {
            docs.push({
                order_id: orderId,
                order_item_id: orderItemId,
                product_id: productId,
                price,
                freight_value: DEFAULT_FREIGHT_PER_ITEM,
                customer_id: customerId,
                created_at: new Date(),
            });
            orderItemId++;
        }

        total += qty * (price + DEFAULT_FREIGHT_PER_ITEM);
    }

    if (!docs.length) {
        return res.status(400).json({ error: "No valid items to insert for this order" });
    }

    try {
        // Measure pure Mongo time
        const { dbMs } = await timeDb(() =>
            mongoDb.collection("orders").insertMany(docs)
        );

        const mongoDbMs = dbMs;
        res.status(201).json({ orderId, total, mongoDbMs });
    } catch (e) {
        res.status(400).json({ error: e.message });
    }
});

// ---------------------- reviews for product (Mongo) ----------------------
app.get("/api/reviews/:productId", async (req, res) => {
    try {
        const productId = String(req.params.productId);
        const limit = parseInt(req.query.limit || 10, 10);
        const skip = parseInt(req.query.skip || 0, 10);

        const pipeline = [
            { $match: { product_id: productId, review_comment_message: { $ne: null } } },
            {
                $project: {
                    review_id: 1,
                    review_score: 1,
                    review_comment_title: 1,
                    review_comment_message: 1,
                    review_creation_date: 1
                }
            },
            { $sort: { review_creation_date: -1 } },
            { $skip: skip },
            { $limit: limit }
        ];

        const { result, dbMs } = await timeDb(() =>
            mongoDb.collection("orders").aggregate(pipeline).toArray()
        );

        const reviews = result.map(doc => ({
            review_id: doc.review_id,
            score: Number(doc.review_score),
            title: doc.review_comment_title || "",
            message: doc.review_comment_message || "",
            creation_date: doc.review_creation_date ? new Date(doc.review_creation_date).toISOString() : null
        }));

        res.json({ reviews, mongoDbMs: dbMs, hasMore: result.length === limit });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ---------------- MongoDB command console ----------------
app.post("/api/mongodb/command", async (req, res) => {
    try {
        const { collection, method, args = [] } = req.body || {};
        if (!collection || !method) {
            return res.status(400).json({ error: "collection and method required" });
        }

        const coll = mongoDb.collection(collection);
        if (typeof coll[method] !== "function") {
            return res.status(400).json({ error: `Unsupported method: ${method}` });
        }

        const { result, dbMs } = await timeDb(async () => {
            const out = await coll[method](...args);
            if (out && typeof out.toArray === "function") return await out.toArray();
            return out;
        });

        res.json({ result, dbMs });
    } catch (e) {
        res.status(400).json({ error: e.message });
    }
});

// ---------------- keep SQL command endpoint for comparison ----------------
app.post("/api/postgres/query", async (req, res) => {
    const q = String(req.body?.query || "");
    if (!q.trim()) return res.status(400).json({ error: "query required" });

    try {
        const { result } = await timeDb(() => pgPool.query(q));
        res.json({ result });
    } catch (e) {
        res.status(400).json({ error: e.message });
    }
});

app.listen(PORT, () => {
    console.log(`Mongo API listening on :${PORT}`);
});
