// scripts/seed_postgres.js
// Usage:
//   POSTGRES_URL="postgres://user:pass@host:5432/dbname" node scripts/seed_postgres.js
//
// This seeder uses the exact CSV filenames you placed in ./data and maps them to
// the tables in schema.sql. It auto-detects comma vs semicolon delimiters.

import fs from "fs";
import path from "path";
import pg from "pg";
import { parse } from "csv-parse/sync";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.resolve(projectRoot, "data");
const schemaPath = path.resolve(projectRoot, "schema.sql");

const POSTGRES_URL = process.env.POSTGRES_URL || process.env.DATABASE_URL;
if (!POSTGRES_URL) {
    console.error("❌ Missing POSTGRES_URL environment variable.");
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
    return true;
}

const pool = new pg.Pool({
    connectionString: POSTGRES_URL,
    ssl: shouldUseSsl() ? { rejectUnauthorized: false } : false,
});

function normKey(s = "") {
    return String(s || "")
        .replace(/^\uFEFF/, "")
        .replace(/^"+|"+$/g, "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, "_")
        .replace(/[^\w_]/g, "");
}

function detectDelimiter(headerLine) {
    // prefer semicolon if present and appears more than comma
    if (!headerLine) return ",";
    const semi = (headerLine.match(/;/g) || []).length;
    const comma = (headerLine.match(/,/g) || []).length;
    return semi > comma ? ";" : ",";
}

function readCsvFlexible(filename) {
    const full = path.join(dataDir, filename);
    if (!fs.existsSync(full)) {
        console.log(`[CSV] Missing: ${filename}`);
        return [];
    }
    const raw = fs.readFileSync(full, "utf8");
    // detect delimiter from first line
    const firstLine = raw.split(/\r?\n/, 2)[0] || "";
    const delimiter = detectDelimiter(firstLine);
    const rows = parse(raw, { columns: true, skip_empty_lines: true, relax_column_count: true, delimiter });
    // normalize keys to snake_case / safe names
    return rows.map(r => {
        const out = {};
        for (const k of Object.keys(r)) {
            out[normKey(k)] = r[k] === "" ? null : r[k];
        }
        return out;
    });
}

async function runDDL() {
    if (!fs.existsSync(schemaPath)) {
        console.log("[DDL] schema.sql not found; skipping DDL.");
        return;
    }
    let sql = fs.readFileSync(schemaPath, "utf8");
    // remove COPY / \copy lines referencing /tmp (we load CSVs from ./data)
    sql = sql.replace(/^COPY[\s\S]*?FROM\s+'\/tmp[\s\S]*?;\s*$/gmi, "");
    sql = sql.replace(/^\\copy[\s\S]*?;\s*$/gmi, "");
    console.log("[DDL] Running schema.sql ...");
    await pool.query(sql);
    console.log("[DDL] DDL complete.");
}

async function insertBatch(table, cols, objs, opts = {}) {
    if (!objs || !objs.length) {
        console.log(`[DB] ${table}: no rows to insert`);
        return 0;
    }
    const batchSize = 500;
    for (let i = 0; i < objs.length; i += batchSize) {
        const chunk = objs.slice(i, i + batchSize);
        const placeholders = [];
        const params = [];
        let p = 1;
        for (const row of chunk) {
            placeholders.push("(" + cols.map(() => `$${p++}`).join(",") + ")");
            for (const c of cols) {
                let v = row[c];
                if (opts.ints && opts.ints.has(c)) v = v == null ? null : parseInt(v, 10);
                if (opts.floats && opts.floats.has(c)) v = v == null ? null : parseFloat(v);
                if (opts.dates && opts.dates.has(c) && v != null) v = new Date(v).toISOString();
                params.push(v == null ? null : v);
            }
        }
        const sql = `INSERT INTO ${table} (${cols.join(",")}) VALUES ${placeholders.join(",")} ON CONFLICT DO NOTHING;`;
        await pool.query(sql, params);
        console.log(`[DB] ${table}: inserted ${Math.min(i + batchSize, objs.length)}/${objs.length}`);
    }
    return objs.length;
}

async function main() {
    try {
        console.log("Project root:", projectRoot);
        console.log("Data dir:", dataDir);
        console.log("Schema:", fs.existsSync(schemaPath) ? schemaPath : "(not found)");
        if (!fs.existsSync(dataDir)) {
            console.error("Data directory ./data does not exist. Put your CSV files there.");
            process.exit(1);
        }

        await runDDL();

        // --- Read CSVs (exact filenames you uploaded) ---
        const csvFiles = {
            order_item_core: "order_item_core.csv",
            order_item_pricing: "order_item_pricing.csv",
            order_item_shipping: "order_item_shipping.csv",
            order_payment_amount: "order_payment_amount.csv",
            order_payment_method: "order_payment_method.csv",
            order_review_core: "order_review_core.csv",
            order_review_text: "order_review_text.csv",
            orders_header: "orders_header.csv",
            orders_timestamps: "orders_timestamps.csv",
            orders_combined: "orders.csv", // optional combined file if you have it
        };

        const orderItemCore = readCsvFlexible(csvFiles.order_item_core);
        const orderItemPricing = readCsvFlexible(csvFiles.order_item_pricing);
        const orderItemShipping = readCsvFlexible(csvFiles.order_item_shipping);
        const orderPaymentAmount = readCsvFlexible(csvFiles.order_payment_amount);
        const orderPaymentMethod = readCsvFlexible(csvFiles.order_payment_method);
        const orderReviewCore = readCsvFlexible(csvFiles.order_review_core);
        const orderReviewText = readCsvFlexible(csvFiles.order_review_text);
        const ordersHeader = readCsvFlexible(csvFiles.orders_header);
        const ordersTimestamps = readCsvFlexible(csvFiles.orders_timestamps);
        const ordersCombined = readCsvFlexible(csvFiles.orders_combined);

        // === 1) Ensure payment type dim exists (derive from payment method file) ===
        if (orderPaymentMethod.length) {
            const types = [...new Set(orderPaymentMethod.map(p => (p.payment_type || "").trim()).filter(Boolean))];
            if (types.length) {
                const rows = types.map(t => ({ payment_type: t }));
                await insertBatch("payment_type_dim", ["payment_type"], rows);
                console.log("[seed] payment_type_dim seeded");
            }
        } else {
            console.log("[seed] order_payment_method.csv missing/empty");
        }

        // === 2) orders_header ===
        if (ordersHeader.length) {
            const headers = ordersHeader.map(r => ({
                order_id: r.order_id,
                customer_id: r.customer_id ?? null,
                order_status: r.order_status ?? null
            }));
            await insertBatch("orders_header", ["order_id", "customer_id", "order_status"], headers);
            console.log("[seed] orders_header inserted");
        } else if (ordersCombined.length) {
            // try to extract header fields from combined orders.csv if present
            const headers = ordersCombined.map(r => ({
                order_id: r.order_id,
                customer_id: r.customer_id ?? null,
                order_status: r.order_status ?? null
            })).filter(x => x.order_id);
            if (headers.length) {
                await insertBatch("orders_header", ["order_id", "customer_id", "order_status"], headers);
                console.log("[seed] orders_header (from orders.csv) inserted");
            }
        } else {
            console.log("[seed] no orders_header.csv and no orders.csv fallback found");
        }

        // === 3) orders_timestamps ===
        if (ordersTimestamps.length) {
            // pick known timestamp columns (normalize)
            const tsCols = ["order_id", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"];
            const rows = ordersTimestamps.map(r => {
                const out = {};
                for (const c of tsCols) out[c] = r[c] ?? null;
                return out;
            });
            await insertBatch("orders_timestamps", tsCols, rows, { dates: new Set(tsCols.slice(1)) });
            console.log("[seed] orders_timestamps inserted");
        } else if (ordersCombined.length) {
            // fallback: try to get timestamp columns from orders.csv
            const tsCols = ["order_id", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"];
            const rows = ordersCombined.map(r => {
                const out = {};
                for (const c of tsCols) out[c] = r[c] ?? null;
                return out;
            }).filter(x => x.order_id);
            if (rows.length) {
                await insertBatch("orders_timestamps", tsCols, rows, { dates: new Set(tsCols.slice(1)) });
                console.log("[seed] orders_timestamps (from orders.csv) inserted");
            }
        } else {
            console.log("[seed] orders_timestamps missing and no fallback");
        }

        // === 4) order item core + pricing + shipping ===
        if (orderItemCore.length) {
            const coreCols = ["order_id", "order_item_id", "product_id", "seller_id"];
            const coreRows = orderItemCore.map(r => {
                return {
                    order_id: r.order_id,
                    order_item_id: r.order_item_id,
                    product_id: r.product_id,
                    seller_id: r.seller_id ?? null
                };
            });
            await insertBatch("order_items_core", coreCols, coreRows);
            console.log("[seed] order_items_core inserted");
        } else {
            console.log("[seed] order_item_core.csv missing/empty");
        }

        if (orderItemPricing.length) {
            const priceCols = ["order_id", "order_item_id", "price", "freight_value"];
            const priceRows = orderItemPricing.map(r => ({
                order_id: r.order_id,
                order_item_id: r.order_item_id,
                price: r.price == null ? null : r.price,
                freight_value: r.freight_value == null ? null : r.freight_value
            }));
            await insertBatch("order_item_pricing", priceCols, priceRows, { floats: new Set(["price", "freight_value"]) });
            console.log("[seed] order_item_pricing inserted");
        } else {
            console.log("[seed] order_item_pricing.csv missing/empty");
        }

        if (orderItemShipping.length) {
            // shipping_limit_date -> order_item_shipping table (if exists)
            const shipCols = ["order_id", "order_item_id", "shipping_limit_date"];
            const shipRows = orderItemShipping.map(r => ({
                order_id: r.order_id,
                order_item_id: r.order_item_id,
                shipping_limit_date: r.shipping_limit_date ?? null
            }));
            await insertBatch("order_item_shipping", shipCols, shipRows, { dates: new Set(["shipping_limit_date"]) });
            console.log("[seed] order_item_shipping inserted");
        } else {
            console.log("[seed] order_item_shipping.csv missing/empty");
        }

        // === 5) payments: method and amount ===
        if (orderPaymentMethod.length) {
            const methodCols = ["order_id", "payment_sequential", "payment_type", "payment_installments"];
            const methodRows = orderPaymentMethod.map(r => ({
                order_id: r.order_id,
                payment_sequential: r.payment_sequential ? parseInt(r.payment_sequential, 10) : 1,
                payment_type: r.payment_type,
                payment_installments: r.payment_installments ? parseInt(r.payment_installments, 10) : 1
            }));
            await insertBatch("order_payment_method", methodCols, methodRows, { ints: new Set(["payment_sequential", "payment_installments"]) });
            console.log("[seed] order_payment_method inserted");
        } else {
            console.log("[seed] order_payment_method.csv missing/empty");
        }

        if (orderPaymentAmount.length) {
            const amountCols = ["order_id", "payment_sequential", "payment_value"];
            const amountRows = orderPaymentAmount.map(r => ({
                order_id: r.order_id,
                payment_sequential: r.payment_sequential ? parseInt(r.payment_sequential, 10) : 1,
                payment_value: r.payment_value == null ? null : parseFloat(r.payment_value)
            }));
            await insertBatch("order_payment_amount", amountCols, amountRows, { floats: new Set(["payment_value"]), ints: new Set(["payment_sequential"]) });
            console.log("[seed] order_payment_amount inserted");
        } else {
            console.log("[seed] order_payment_amount.csv missing/empty");
        }

        // === 6) reviews: core + text ===
        if (orderReviewCore.length) {
            // header used semicolons in your file; keys normalized above
            const coreCols = ["review_id", "order_id", "review_score", "review_creation_date", "review_answer_timestamp"];
            const coreRows = orderReviewCore.map(r => ({
                review_id: r.review_id,
                order_id: r.order_id,
                review_score: r.review_score == null ? null : parseInt(r.review_score, 10),
                review_creation_date: r.review_creation_date ?? null,
                review_answer_timestamp: r.review_answer_timestamp ?? null
            }));
            await insertBatch("order_review_core", coreCols, coreRows, { ints: new Set(["review_score"]), dates: new Set(["review_creation_date", "review_answer_timestamp"]) });
            console.log("[seed] order_review_core inserted");
        } else {
            console.log("[seed] order_review_core.csv missing/empty");
        }

        if (orderReviewText.length) {
            const textCols = ["review_id", "order_id", "review_comment_title", "review_comment_message"];
            const textRows = orderReviewText.map(r => ({
                review_id: r.review_id,
                order_id: r.order_id,
                review_comment_title: r.review_comment_title ?? null,
                review_comment_message: r.review_comment_message ?? null
            }));
            await insertBatch("order_review_text", textCols, textRows);
            console.log("[seed] order_review_text inserted");
        } else {
            console.log("[seed] order_review_text.csv missing/empty");
        }

        console.log("[seed] All done. Inspect pgAdmin4 to verify table rows.");
        await pool.end();
        process.exit(0);
    } catch (err) {
        console.error("[seed] Error:", err);
        try { await pool.end(); } catch (e) { }
        process.exit(2);
    }
}

main();
