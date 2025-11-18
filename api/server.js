// api-server.js  (ESM)
// Works with your existing ERD: no new tables.
// Uses normalized tables if present; otherwise falls back to staging `orders` table.

import express from "express";
import cors from "cors";
import pg from "pg";
import crypto from "crypto";

const PORT = process.env.PORT || 3001;
const POSTGRES_URL = process.env.POSTGRES_URL || process.env.DATABASE_URL;
if (!POSTGRES_URL) {
    console.error("❌ Missing POSTGRES_URL env var.");
    process.exit(1);
}

function shouldUseSsl() {
    if (process.env.POSTGRES_USE_SSL === "true") return true;
    if (process.env.POSTGRES_USE_SSL === "false") return false;
    try {
        const u = new URL(POSTGRES_URL);
        const h = u.hostname;
        if (h === "localhost" || h === "127.0.0.1") return false;
    } catch { }
    return true; // default SSL for non-local DBs
}
const pool = new pg.Pool({ connectionString: POSTGRES_URL, ssl: shouldUseSsl() ? { rejectUnauthorized: false } : false });

const app = express();
app.use(cors());
app.use(express.json());

// ---------------------- helpers ----------------------
const toUiProduct = (row) => ({
    id: row.id,                 // product_id becomes the product id
    sku: row.id,
    title: row.id,              // no name table -> show product_id; you can prettify if you want
    name: row.id,
    price: row.price !== null ? Number(row.price) : null,
    order_count: Number(row.order_count || 0),
});

async function tableExists(name) {
    const q = `SELECT to_regclass($1) AS t`;
    const { rows } = await pool.query(q, [name]);
    return !!rows[0].t;
}

// pick normalized path if both tables exist, else staging path
async function pickProductSource() {
    const hasCore = await tableExists("order_items_core");
    const hasPricing = await tableExists("order_item_pricing");
    if (hasCore && hasPricing) return "normalized";
    const hasStaging = await tableExists("orders"); // flat orders.csv staging table
    return hasStaging ? "staging" : "none";
}

// ---------------------- health -----------------------
app.get("/api/health", async (_req, res) => {
    try { await pool.query("SELECT 1"); res.json({ ok: true }); }
    catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ---------------------- products list ----------------
app.get("/api/products", async (req, res) => {
    const { search = "", limit = 50, offset = 0 } = req.query;
    try {
        const src = await pickProductSource();
        if (src === "none") return res.json([]); // nothing loaded yet

        let rows;
        if (src === "normalized") {
            // order_items_core(product_id) + order_item_pricing(price)
            // GROUP to form "catalog" without a products table.
            const sql = `
        SELECT oic.product_id AS id,
               ROUND(AVG(oip.price)::numeric, 2) AS price,
               COUNT(*) AS order_count
        FROM order_items_core oic
        JOIN order_item_pricing oip
          ON oip.order_id = oic.order_id AND oip.order_item_id = oic.order_item_id
        WHERE ($1 = '' OR oic.product_id ILIKE '%'||$1||'%')
        GROUP BY oic.product_id
        ORDER BY order_count DESC
        LIMIT $2 OFFSET $3`;
            const { rows: r } = await pool.query(sql, [String(search).trim(), +limit, +offset]);
            rows = r;
        } else {
            // staging `orders` table has product_id + price (from orders.csv)
            const sql = `
        SELECT product_id AS id,
               ROUND(AVG(price)::numeric, 2) AS price,
               COUNT(*) AS order_count
        FROM orders
        WHERE product_id IS NOT NULL
          AND ($1 = '' OR product_id ILIKE '%'||$1||'%')
        GROUP BY product_id
        ORDER BY order_count DESC
        LIMIT $2 OFFSET $3`;
            const { rows: r } = await pool.query(sql, [String(search).trim(), +limit, +offset]);
            rows = r;
        }
        res.json(rows.map(toUiProduct));
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ---------------------- product detail ----------------
app.get("/api/products/:id", async (req, res) => {
    const pid = String(req.params.id);
    try {
        const src = await pickProductSource();
        if (src === "none") return res.status(404).json({ error: "Not found" });

        let product;
        if (src === "normalized") {
            const sql = `
        SELECT oic.product_id AS id,
               ROUND(AVG(oip.price)::numeric, 2) AS price,
               COUNT(*) AS order_count
        FROM order_items_core oic
        JOIN order_item_pricing oip
          ON oip.order_id = oic.order_id AND oip.order_item_id = oic.order_item_id
        WHERE oic.product_id = $1
        GROUP BY oic.product_id`;
            const { rows } = await pool.query(sql, [pid]);
            if (!rows.length) return res.status(404).json({ error: "Not found" });
            product = rows[0];
        } else {
            const sql = `
        SELECT product_id AS id,
               ROUND(AVG(price)::numeric, 2) AS price,
               COUNT(*) AS order_count
        FROM orders
        WHERE product_id = $1
        GROUP BY product_id`;
            const { rows } = await pool.query(sql, [pid]);
            if (!rows.length) return res.status(404).json({ error: "Not found" });
            product = rows[0];
        }

        res.json(toUiProduct(product));
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ---------------------- admin upsert "product" ----------------------
// There is no products table; instead, we accept this call and do nothing DB-wise,
// just return a synthesized object so your existing admin UI doesn't break.
// (If you prefer, you can 400 this endpoint. Keeping it "no-op" is gentler.)
app.post("/api/products", async (req, res) => {
    const { sku, title, price } = req.body || {};
    if (!sku) return res.status(400).json({ error: "sku required (maps to product_id)" });
    res.status(201).json(toUiProduct({ id: sku, price: price ?? null, order_count: 0 }));
});

// ---------------------- checkout -> insert normalized rows ----------------------
// We will insert into orders_header, orders_timestamps, order_items_core, order_item_pricing,
// and (optionally) order_payment_method + order_payment_amount.
//
// Table definitions referenced here:
// - orders_header(order_id, customer_id, order_status) (customer_id/order_status NOT NULL) :contentReference[oaicite:3]{index=3}
// - orders_timestamps(order_id, ... timestamps ...) with FK to orders_header later :contentReference[oaicite:4]{index=4} :contentReference[oaicite:5]{index=5}
// - order_items_core(order_id, order_item_id, product_id, seller_id) (all NOT NULL) :contentReference[oaicite:6]{index=6}
// - order_item_pricing(order_id, order_item_id, price, freight_value) (all NOT NULL) :contentReference[oaicite:7]{index=7}
// - payment_type_dim(payment_type) + order_payment_method + order_payment_amount (FK chain) :contentReference[oaicite:8]{index=8} :contentReference[oaicite:9]{index=9} :contentReference[oaicite:10]{index=10}
app.post("/api/orders", async (req, res) => {
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.status(400).json({ error: "items required" });

    // synthesize order id (32-char hex)
    const orderId = crypto.randomBytes(16).toString("hex");
    const customerId = (req.body?.customer_id && String(req.body.customer_id)) || "guest0000000000000000000000000000";
    const orderStatus = "created";

    try {
        await pool.query("BEGIN");

        // Header + timestamps
        await pool.query(
            "INSERT INTO orders_header (order_id, customer_id, order_status) VALUES ($1,$2,$3)",
            [orderId, customerId, orderStatus]
        );
        await pool.query(
            `INSERT INTO orders_timestamps (order_id, order_purchase_timestamp)
       VALUES ($1, NOW())`,
            [orderId]
        );

        // Items (use index as order_item_id), price per line from request or inferred
        let total = 0;
        for (let i = 0; i < items.length; i++) {
            const it = items[i] || {};
            const pid = String(it.id || it.product_id || "");
            const qty = parseInt(it.qty ?? it.quantity ?? 1, 10);
            if (!pid || !(qty > 0)) throw new Error("Invalid item");

            // infer a unit price if not provided by taking AVG from normalized or staging
            let unitPrice = Number(it.price);
            if (!Number.isFinite(unitPrice)) {
                const src = await pickProductSource();
                if (src === "normalized") {
                    const { rows } = await pool.query(
                        `SELECT AVG(oip.price) AS p
             FROM order_items_core oic
             JOIN order_item_pricing oip
               ON oip.order_id=oic.order_id AND oip.order_item_id=oic.order_item_id
             WHERE oic.product_id=$1`, [pid]);
                    unitPrice = rows[0]?.p ? Number(rows[0].p) : 0;
                } else if (src === "staging") {
                    const { rows } = await pool.query(
                        `SELECT AVG(price) AS p FROM orders WHERE product_id=$1`, [pid]);
                    unitPrice = rows[0]?.p ? Number(rows[0].p) : 0;
                } else {
                    unitPrice = 0;
                }
            }

            // Insert each unit as a separate order_item_id entry to keep schema simple
            for (let c = 0; c < qty; c++) {
                const itemId = i * 1000 + c + 1; // simple unique per order
                await pool.query(
                    "INSERT INTO order_items_core (order_id, order_item_id, product_id, seller_id) VALUES ($1,$2,$3,$4)",
                    [orderId, itemId, pid, "SELLER00000000000000000000000000"]
                );
                await pool.query(
                    "INSERT INTO order_item_pricing (order_id, order_item_id, price, freight_value) VALUES ($1,$2,$3,$4)",
                    [orderId, itemId, unitPrice, 0]
                );
                total += unitPrice;
            }
        }

        // minimal payments (optional but nice)
        // ensure a payment type exists (FK requires payment_type_dim row)
        await pool.query(
            "INSERT INTO payment_type_dim (payment_type) VALUES ($1) ON CONFLICT DO NOTHING",
            ["manual"]
        );
        await pool.query(
            "INSERT INTO order_payment_method (order_id, payment_sequential, payment_type, payment_installments) VALUES ($1,1,$2,1)",
            [orderId, "manual"]
        );
        await pool.query(
            "INSERT INTO order_payment_amount (order_id, payment_sequential, payment_value) VALUES ($1,1,$2)",
            [orderId, total]
        );

        await pool.query("COMMIT");
        res.status(201).json({ orderId, total });
    } catch (e) {
        await pool.query("ROLLBACK");
        res.status(400).json({ error: e.message });
    }
});

// ---------------------- raw SQL (for commands.html) ----------------------
app.post("/api/postgres/query", async (req, res) => {
    const q = String(req.body?.query || "");
    if (!q.trim()) return res.status(400).json({ error: "query required" });
    try {
        const result = await pool.query(q);
        res.json({ result });
    } catch (e) {
        res.status(400).json({ error: e.message });
    }
});

app.listen(PORT, () => {
    console.log(`API listening on :${PORT} (no new tables; derived products from existing orders data)`);
});
