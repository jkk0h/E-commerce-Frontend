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

const pool = new pg.Pool({
    connectionString: POSTGRES_URL,
    ssl: shouldUseSsl() ? { rejectUnauthorized: false } : false,
});
pool.query("SELECT NOW()")
    .then(() => console.log("Postgres connected successfully"))
    .catch(err => console.error("Postgres connection error:", err.message));

const app = express();
app.use(cors());
app.use(express.json());

// ---------------------- helpers ----------------------
const toUiProduct = (row) => {
    const pid = row.id; // this is product_id in your SQL
    return {
        id: pid,
        product_id: pid,
        title: pid,
        name: pid,
        price: row.price !== null ? Number(row.price) : null,
        order_count: Number(row.order_count || 0),
    };
};

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
        if (src === "none") return res.json([]);

        let rows;
        const searchTerm = String(search).trim();
        const lim = +limit;
        const off = +offset;

        if (src === "normalized") {
            // Normalized path: join through orders_timestamps to get the latest purchase time
            const sql = `
                SELECT
                    oic.product_id AS id,
                    ROUND(AVG(oip.price)::numeric, 2) AS price,
                    COUNT(*) AS order_count,
                    MAX(ot.order_purchase_timestamp) AS last_order_ts
                FROM order_items_core oic
                JOIN order_item_pricing oip
                  ON oip.order_id = oic.order_id
                 AND oip.order_item_id = oic.order_item_id
                JOIN orders_timestamps ot
                  ON ot.order_id = oic.order_id
                WHERE ($1 = '' OR oic.product_id ILIKE '%' || $1 || '%')
                GROUP BY oic.product_id
                ORDER BY last_order_ts DESC NULLS LAST
                LIMIT $2 OFFSET $3
            `;
            const { rows: r } = await pool.query(sql, [searchTerm, lim, off]);
            rows = r;
        } else {
            // Staging path: use the flat orders table, which already has order_purchase_timestamp
            const sql = `
                SELECT
                    product_id AS id,
                    ROUND(AVG(price)::numeric, 2) AS price,
                    COUNT(*) AS order_count,
                    MAX(order_purchase_timestamp) AS last_order_ts
                FROM orders
                WHERE product_id IS NOT NULL
                  AND ($1 = '' OR product_id ILIKE '%' || $1 || '%')
                GROUP BY product_id
                ORDER BY last_order_ts DESC NULLS LAST
                LIMIT $2 OFFSET $3
            `;
            const { rows: r } = await pool.query(sql, [searchTerm, lim, off]);
            rows = r;
        }

        // toUiProduct ignores last_order_ts, which is fine; we only use it for sorting
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

// ---------------------- admin seed product ----------------------
// ---------------------- admin seed product ----------------------
app.post("/api/products", async (req, res) => {
    const {
        product_id,
        sku,
        title,
        price,
        quantity,
        seller_id,
        admin_id,
    } = req.body || {};

    const rawPid =
        (product_id && String(product_id).trim()) ||
        (sku && String(sku).trim()) ||
        "";

    if (!rawPid) {
        return res.status(400).json({ error: "product_id required (or sku)" });
    }

    const productId = rawPid.slice(0, 32);
    const qty = Math.max(1, parseInt(quantity ?? 1, 10) || 1);
    const unitPrice = price != null ? Number(price) : 0;

    if (!Number.isFinite(unitPrice) || unitPrice < 0) {
        return res
            .status(400)
            .json({ error: "price must be a non-negative number" });
    }

    const hasCore = await tableExists("order_items_core");
    const hasPricing = await tableExists("order_item_pricing");
    if (!hasCore || !hasPricing) {
        return res.status(400).json({
            error:
                "Normalized tables order_items_core/order_item_pricing not found; admin seeding requires normalized schema.",
        });
    }

    // ---- NEW: Identity wiring ----
    const DEFAULT_ADMIN_ID = "admin_seed_product_0000000000000";
    const DEFAULT_SELLER_ID = "seller000000000000000000000000";

    const rawAdminId =
        admin_id && String(admin_id).trim().length
            ? String(admin_id).trim()
            : null;
    const rawSellerId =
        seller_id && String(seller_id).trim().length
            ? String(seller_id).trim()
            : null;

    // Use admin_id as customer_id in orders_header
    const customerId = (rawAdminId || DEFAULT_ADMIN_ID).slice(0, 32);
    // Use seller_id in order_items_core.seller_id
    const sellerId = (rawSellerId || DEFAULT_SELLER_ID).slice(0, 32);

    const orderId = crypto.randomBytes(16).toString("hex");
    const orderStatus = "seed";

    const DEFAULT_FREIGHT_PER_ITEM = 0;

    const t0 = process.hrtime.bigint();

    try {
        await pool.query("BEGIN");

        await pool.query(
            "INSERT INTO orders_header (order_id, customer_id, order_status) VALUES ($1,$2,$3)",
            [orderId, customerId, orderStatus]
        );
        await pool.query(
            "INSERT INTO orders_timestamps (order_id, order_purchase_timestamp) VALUES ($1, NOW())",
            [orderId]
        );

        let orderItemId = 1;
        let total = 0;

        for (let i = 0; i < qty; i++) {
            await pool.query(
                `INSERT INTO order_items_core (order_id, order_item_id, product_id, seller_id)
                 VALUES ($1, $2, $3, $4)`,
                [orderId, orderItemId, productId, sellerId]
            );

            await pool.query(
                `INSERT INTO order_item_pricing (order_id, order_item_id, price, freight_value)
                 VALUES ($1, $2, $3, $4)`,
                [orderId, orderItemId, unitPrice, DEFAULT_FREIGHT_PER_ITEM]
            );

            total += unitPrice + DEFAULT_FREIGHT_PER_ITEM;
            orderItemId++;
        }

        if (qty <= 0) {
            throw new Error("No valid units to insert for this product");
        }

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

        const tEnd = process.hrtime.bigint();
        const sqlDbMs = Number(tEnd - t0) / 1e6;
        const backendTotalMs = sqlDbMs;

        const productForUi = toUiProduct({
            id: productId,
            price: unitPrice,
            order_count: qty,
        });

        return res.status(201).json({
            ok: true,
            product: productForUi,
            orderId,
            total,
            sqlDbMs,
            backendTotalMs,
        });
    } catch (e) {
        try {
            await pool.query("ROLLBACK");
        } catch { }
        const t1 = process.hrtime.bigint();
        const backendTotalMs = Number(t1 - t0) / 1e6;
        return res.status(400).json({ error: e.message, backendTotalMs });
    }
});


// ---------------------- SQL command console ----------------------
app.post("/api/postgres/query", async (req, res) => {
    const q = String(req.body?.query || "");
    if (!q.trim()) {
        return res.status(400).json({ error: "query required" });
    }

    const t0 = process.hrtime.bigint();
    try {
        const result = await pool.query(q);
        const t1 = process.hrtime.bigint();
        const dbMs = Number(t1 - t0) / 1e6;

        res.json({
            result,
            dbMs
        });
    } catch (e) {
        const t1 = process.hrtime.bigint();
        const dbMs = Number(t1 - t0) / 1e6;
        res.status(400).json({ error: e.message, dbMs });
    }
});

// ---------------------- checkout ----------------------
app.post("/api/orders", async (req, res) => {
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.status(400).json({ error: "items required" });

    const orderId = crypto.randomBytes(16).toString("hex");

    const rawCustomerId =
        req.body?.customer_id && String(req.body.customer_id).trim().length
            ? String(req.body.customer_id).trim()
            : "guest000000000000000000000000000";

    const customerId = rawCustomerId.slice(0, 32);
    const orderStatus = "created";

    const t0 = process.hrtime.bigint();

    try {
        await pool.query("BEGIN");

        await pool.query(
            "INSERT INTO orders_header (order_id, customer_id, order_status) VALUES ($1,$2,$3)",
            [orderId, customerId, orderStatus]
        );
        await pool.query(
            "INSERT INTO orders_timestamps (order_id, order_purchase_timestamp) VALUES ($1, NOW())",
            [orderId]
        );

        let total = 0;
        let orderItemId = 1;
        const DEFAULT_SELLER_ID = "seller000000000000000000000000";
        const DEFAULT_FREIGHT_PER_ITEM = 0;

        for (const item of items) {
            const productIdRaw = item?.id != null ? String(item.id).trim() : "";
            const qty = Math.max(1, parseInt(item?.quantity, 10) || 1);
            const price = item?.price != null ? Number(item.price) : 0;

            if (!productIdRaw) continue;
            if (!Number.isFinite(price)) continue;

            const productId = productIdRaw.slice(0, 32);

            for (let i = 0; i < qty; i++) {
                await pool.query(
                    `INSERT INTO order_items_core (order_id, order_item_id, product_id, seller_id)
                     VALUES ($1, $2, $3, $4)`,
                    [orderId, orderItemId, productId, DEFAULT_SELLER_ID]
                );

                await pool.query(
                    `INSERT INTO order_item_pricing (order_id, order_item_id, price, freight_value)
                     VALUES ($1, $2, $3, $4)`,
                    [orderId, orderItemId, price, DEFAULT_FREIGHT_PER_ITEM]
                );

                orderItemId++;
            }

            total += qty * (price + DEFAULT_FREIGHT_PER_ITEM);
        }

        if (orderItemId === 1) {
            throw new Error("No valid items to insert for this order");
        }

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

        const tEnd = process.hrtime.bigint();
        const sqlDbMs = Number(tEnd - t0) / 1e6;
        const backendTotalMs = sqlDbMs;

        res.status(201).json({
            ok: true,
            orderId,
            total,
            sqlDbMs,
            backendTotalMs
        });
    } catch (e) {
        console.error("Order error:", e);
        try { await pool.query("ROLLBACK"); } catch { }

        const t1 = process.hrtime.bigint();
        const backendTotalMs = Number(t1 - t0) / 1e6;

        res.status(400).json({ error: e.message, backendTotalMs });
    }
});

// ---------------------- reviews for product (SQL) ----------------------
app.get("/api/reviews/:productId", async (req, res) => {
    try {
        const productId = String(req.params.productId).slice(0, 32);
        const limit = parseInt(req.query.limit || 10, 10);
        const offset = parseInt(req.query.offset || 0, 10);

        const source = await pickProductSource();

        let query, params;
        if (source === "normalized") {
            query = `
                SELECT 
                    rc.review_id, rc.order_id, rc.review_score, 
                    rc.review_creation_date, rc.review_answer_timestamp,
                    rt.review_comment_title, rt.review_comment_message
                FROM order_reviews_core rc
                JOIN order_review_text rt ON rc.review_id = rt.review_id AND rc.order_id = rt.order_id
                JOIN order_items_core oi ON rc.order_id = oi.order_id
                WHERE oi.product_id = $1 AND rt.review_comment_message IS NOT NULL
                ORDER BY rc.review_creation_date DESC
                LIMIT $2 OFFSET $3
            `;
            params = [productId, limit, offset];
        } else if (source === "staging") {
            query = `
                SELECT review_id, order_id, review_score, review_creation_date, review_answer_timestamp,
                       review_comment_title, review_comment_message
                FROM orders
                WHERE product_id = $1 AND review_comment_message IS NOT NULL
                ORDER BY review_creation_date DESC
                LIMIT $2 OFFSET $3
            `;
            params = [productId, limit, offset];
        } else {
            return res.status(404).json({ error: "No product source available" });
        }

        const t0 = process.hrtime.bigint();
        const { rows } = await pool.query(query, params);
        const t1 = process.hrtime.bigint();
        const sqlDbMs = Number(t1 - t0) / 1e6;

        const reviews = rows.map(row => ({
            review_id: row.review_id,
            score: Number(row.review_score),
            title: row.review_comment_title || "",
            message: row.review_comment_message || "",
            creation_date: row.review_creation_date ? row.review_creation_date.toISOString() : null
        }));

        res.json({ reviews, sqlDbMs, hasMore: rows.length === limit });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ---------------------- Get Monthly Stats for Product (Using MV) ----------------------
app.get("/api/monthly-stats/:productId", async (req, res) => {
    try {
        const productId = String(req.params.productId).slice(0, 32);

        // NOTE: no date filter here; returns all months in MV for that product
        const { rows } = await pool.query(
            `SELECT *
             FROM mv_product_monthly_stats
             WHERE product_id = $1
             ORDER BY order_month ASC`,
            [productId]
        );

        res.json({ stats: rows });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});


// ---------------------- start server ----------------------
app.listen(PORT, () => {
    console.log(`SQL API listening on http://localhost:${PORT}`);
});
