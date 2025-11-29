// scripts/seed_postgres.js
// Usage:
//   POSTGRES_URL="postgres://user:pass@host:5432/dbname" node scripts/seed_postgres.js
//
// This seeder looks for each dataset by *basename* in ./data and automatically
// reads CSV, XLSX, or XLS (first worksheet). CSV delimiter (comma/semicolon) is
// auto-detected. Header keys are normalized to snake_case.

import fs from "fs";
import path from "path";
import pg from "pg";
import { parse as parseCsv } from "csv-parse/sync";
import xlsx from "xlsx";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.resolve(projectRoot, "scripts", "data");
const schemaPath = path.resolve(projectRoot, "scripts", "schema.sql");

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

// ---------- helpers: normalization & parsing ----------

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
    if (!headerLine) return ",";
    const semi = (headerLine.match(/;/g) || []).length;
    const comma = (headerLine.match(/,/g) || []).length;
    return semi > comma ? ";" : ",";
}

// Try basename.csv, basename.xlsx, basename.xls inside dataDir
function findDataFile(basename) {
    const candidates = [
        `${basename}.csv`,
        `${basename}.xlsx`,
        `${basename}.xls`,
    ];
    for (const name of candidates) {
        const full = path.join(dataDir, name);
        if (fs.existsSync(full)) return { full, name };
    }
    return null;
}

// Read CSV/XLSX/XLS into array of objects with normalized keys.
// CSV uses delimiter autodetect; Excel uses the first worksheet.
function readTableFlexible(basename) {
    const hit = findDataFile(basename);
    if (!hit) {
        console.log(`[DATA] Missing: ${basename}.[csv|xlsx|xls]`);
        return [];
    }

    const ext = path.extname(hit.full).toLowerCase();
    if (ext === ".csv") {
        const raw = fs.readFileSync(hit.full, "utf8");
        const firstLine = raw.split(/\r?\n/, 2)[0] || "";
        const delimiter = detectDelimiter(firstLine);
        const rows = parseCsv(raw, {
            columns: true,
            skip_empty_lines: true,
            relax_column_count: true,
            delimiter,
        });
        return rows.map((r) => {
            const out = {};
            for (const k of Object.keys(r)) out[normKey(k)] = r[k] === "" ? null : r[k];
            return out;
        });
    }

    // Excel path (.xlsx or .xls)
    const wb = xlsx.readFile(hit.full);
    const ws = wb.Sheets[wb.SheetNames[0]];
    const rows = xlsx.utils.sheet_to_json(ws, { defval: null }); // keep empty cells as null
    return rows.map((r) => {
        const out = {};
        for (const k of Object.keys(r)) out[normKey(k)] = r[k];
        return out;
    });
}

// Run optional schema.sql (with any \copy/COPY to /tmp stripped)
async function runDDL() {
    if (!fs.existsSync(schemaPath)) {
        console.log("[DDL] schema.sql not found; skipping DDL.");
        return;
    }
    let sql = fs.readFileSync(schemaPath, "utf8");
    sql = sql.replace(/^COPY[\s\S]*?FROM\s+'\/tmp[\s\S]*?;\s*$/gmi, "");
    sql = sql.replace(/^\\copy[\s\S]*?;\s*$/gmi, "");
    console.log("[DDL] Running schema.sql ...");
    await pool.query(sql);
    console.log("[DDL] DDL complete.");
}

// Batched inserts with optional type coercions
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

        const sql =
            `INSERT INTO ${table} (${cols.join(",")}) ` +
            `VALUES ${placeholders.join(",")} ON CONFLICT DO NOTHING;`;

        await pool.query(sql, params);
        console.log(
            `[DB] ${table}: inserted ${Math.min(i + batchSize, objs.length)}/${objs.length}`
        );
    }

    return objs.length;
}

async function main() {
    try {
        console.log("Project root:", projectRoot);
        console.log("Data dir:", dataDir);
        console.log("Schema:", fs.existsSync(schemaPath) ? schemaPath : "(not found)");

        if (!fs.existsSync(dataDir)) {
            console.error("Data directory ./data does not exist. Put your files there.");
            process.exit(1);
        }

        // === Run DDL (schema.sql) ===
        await runDDL();

        // === Check materialized view exists ===
        try {
            console.log("[DDL] Checking for materialized view mv_product_monthly_stats ...");
            const mvRes = await pool.query(`
                SELECT matviewname
                FROM pg_matviews
                WHERE schemaname = 'public'
                  AND matviewname = 'mv_product_monthly_stats';
            `);

            if (mvRes.rowCount > 0) {
                console.log("✅ Materialized View exists: mv_product_monthly_stats");
            } else {
                console.log("⚠️  Materialized View NOT found: mv_product_monthly_stats");
            }
        } catch (checkErr) {
            console.error("[DDL] Error while checking materialized view:", checkErr);
        }

        // ---- Basenames only (extension-free) ----
        const names = {
            order_item_core: "order_item_core",
            order_item_pricing: "order_item_pricing",
            order_item_shipping: "order_item_shipping",
            order_payment_amount: "order_payment_amount",
            order_payment_method: "order_payment_method",
            order_review_core: "order_review_core",
            order_review_text: "order_review_text",
            orders_header: "orders_header",
            orders_timestamps: "orders_timestamps",
            orders_combined: "orders", // optional combined orders
        };

        // ---- Load tables (CSV/XLSX/XLS all supported) ----
        const orderItemCore = readTableFlexible(names.order_item_core);
        const orderItemPricing = readTableFlexible(names.order_item_pricing);
        const orderItemShipping = readTableFlexible(names.order_item_shipping);
        const orderPaymentAmount = readTableFlexible(names.order_payment_amount);
        const orderPaymentMethod = readTableFlexible(names.order_payment_method);
        const orderReviewCore = readTableFlexible(names.order_review_core);
        const orderReviewText = readTableFlexible(names.order_review_text);
        const ordersHeader = readTableFlexible(names.orders_header);
        const ordersTimestamps = readTableFlexible(names.orders_timestamps);
        const ordersCombined = readTableFlexible(names.orders_combined);

        // === 1) payment_type_dim from payment method ===
        if (orderPaymentMethod.length) {
            const types = [
                ...new Set(
                    orderPaymentMethod
                        .map((p) => (p.payment_type || "").toString().trim())
                        .filter(Boolean)
                ),
            ];
            if (types.length) {
                const rows = types.map((t) => ({ payment_type: t }));
                await insertBatch("payment_type_dim", ["payment_type"], rows);
                console.log("[seed] payment_type_dim seeded");
            }
        } else {
            console.log("[seed] order_payment_method missing/empty");
        }

        // === 2) orders_header ===
        if (ordersHeader.length) {
            const headers = ordersHeader.map((r) => ({
                order_id: r.order_id,
                customer_id: r.customer_id ?? null,
                order_status: r.order_status ?? null,
            }));
            await insertBatch("orders_header", ["order_id", "customer_id", "order_status"], headers);
            console.log("[seed] orders_header inserted");
        } else if (ordersCombined.length) {
            const headers = ordersCombined
                .map((r) => ({
                    order_id: r.order_id,
                    customer_id: r.customer_id ?? null,
                    order_status: r.order_status ?? null,
                }))
                .filter((x) => x.order_id);
            if (headers.length) {
                await insertBatch("orders_header", ["order_id", "customer_id", "order_status"], headers);
                console.log("[seed] orders_header (from orders) inserted");
            }
        } else {
            console.log("[seed] no orders_header and no orders fallback found");
        }

        // === 3) orders_timestamps ===
        {
            const tsCols = [
                "order_id",
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ];

            if (ordersTimestamps.length) {
                const rows = ordersTimestamps.map((r) => {
                    const out = {};
                    for (const c of tsCols) out[c] = r[c] ?? null;
                    return out;
                });
                await insertBatch("orders_timestamps", tsCols, rows, {
                    dates: new Set(tsCols.slice(1)),
                });
                console.log("[seed] orders_timestamps inserted");
            } else if (ordersCombined.length) {
                const rows = ordersCombined
                    .map((r) => {
                        const out = {};
                        for (const c of tsCols) out[c] = r[c] ?? null;
                        return out;
                    })
                    .filter((x) => x.order_id);
                if (rows.length) {
                    await insertBatch("orders_timestamps", tsCols, rows, {
                        dates: new Set(tsCols.slice(1)),
                    });
                    console.log("[seed] orders_timestamps (from orders) inserted");
                }
            } else {
                console.log("[seed] orders_timestamps missing and no fallback");
            }
        }

        // === 4) order items: core + pricing + shipping ===
        if (orderItemCore.length) {
            const coreCols = ["order_id", "order_item_id", "product_id", "seller_id"];
            const coreRows = orderItemCore.map((r) => ({
                order_id: r.order_id,
                order_item_id: r.order_item_id,
                product_id: r.product_id,
                seller_id: r.seller_id ?? null,
            }));
            await insertBatch("order_items_core", coreCols, coreRows);
            console.log("[seed] order_items_core inserted");
        } else {
            console.log("[seed] order_item_core missing/empty");
        }

        if (orderItemPricing.length) {
            const priceCols = ["order_id", "order_item_id", "price", "freight_value"];
            const priceRows = orderItemPricing.map((r) => ({
                order_id: r.order_id,
                order_item_id: r.order_item_id,
                price: r.price == null ? null : r.price,
                freight_value: r.freight_value == null ? null : r.freight_value,
            }));
            await insertBatch("order_item_pricing", priceCols, priceRows, {
                floats: new Set(["price", "freight_value"]),
            });
            console.log("[seed] order_item_pricing inserted");
        } else {
            console.log("[seed] order_item_pricing missing/empty");
        }

        if (orderItemShipping.length) {
            const shipCols = ["order_id", "order_item_id", "shipping_limit_date"];
            const shipRows = orderItemShipping.map((r) => ({
                order_id: r.order_id,
                order_item_id: r.order_item_id,
                shipping_limit_date: r.shipping_limit_date ?? null,
            }));
            await insertBatch("order_item_shipping", shipCols, shipRows, {
                dates: new Set(["shipping_limit_date"]),
            });
            console.log("[seed] order_item_shipping inserted");
        } else {
            console.log("[seed] order_item_shipping missing/empty");
        }

        // === 5) payments: method and amount ===
        if (orderPaymentMethod.length) {
            const methodCols = ["order_id", "payment_sequential", "payment_type", "payment_installments"];
            const methodRows = orderPaymentMethod.map((r) => ({
                order_id: r.order_id,
                payment_sequential: r.payment_sequential
                    ? parseInt(r.payment_sequential, 10)
                    : 1,
                payment_type: r.payment_type,
                payment_installments: r.payment_installments
                    ? parseInt(r.payment_installments, 10)
                    : 1,
            }));
            await insertBatch("order_payment_method", methodCols, methodRows, {
                ints: new Set(["payment_sequential", "payment_installments"]),
            });
            console.log("[seed] order_payment_method inserted");
        } else {
            console.log("[seed] order_payment_method missing/empty");
        }

        if (orderPaymentAmount.length) {
            const amountCols = ["order_id", "payment_sequential", "payment_value"];
            const amountRows = orderPaymentAmount.map((r) => ({
                order_id: r.order_id,
                payment_sequential: r.payment_sequential
                    ? parseInt(r.payment_sequential, 10)
                    : 1,
                payment_value: r.payment_value == null ? null : parseFloat(r.payment_value),
            }));
            await insertBatch("order_payment_amount", amountCols, amountRows, {
                floats: new Set(["payment_value"]),
                ints: new Set(["payment_sequential"]),
            });
            console.log("[seed] order_payment_amount inserted");
        } else {
            console.log("[seed] order_payment_amount missing/empty");
        }

        // === 6) reviews: core + text ===
        if (orderReviewCore.length) {
            const coreCols = [
                "review_id",
                "order_id",
                "review_score",
                "review_creation_date",
                "review_answer_timestamp",
            ];
            const coreRows = orderReviewCore.map((r) => ({
                review_id: r.review_id,
                order_id: r.order_id,
                review_score: r.review_score == null ? null : parseInt(r.review_score, 10),
                review_creation_date: r.review_creation_date ?? null,
                review_answer_timestamp: r.review_answer_timestamp ?? null,
            }));
            await insertBatch("order_reviews_core", coreCols, coreRows, {
                ints: new Set(["review_score"]),
                dates: new Set(["review_creation_date", "review_answer_timestamp"]),
            });
            console.log("[seed] order_reviews_core inserted");
        } else {
            console.log("[seed] order_review_core missing/empty");
        }

        if (orderReviewText.length) {
            const textCols = [
                "review_id",
                "order_id",
                "review_comment_title",
                "review_comment_message",
            ];
            const textRows = orderReviewText.map((r) => ({
                review_id: r.review_id,
                order_id: r.order_id,
                review_comment_title: r.review_comment_title ?? null,
                review_comment_message: r.review_comment_message ?? null,
            }));
            await insertBatch("order_review_text", textCols, textRows);
            console.log("[seed] order_review_text inserted");
        } else {
            console.log("[seed] order_review_text missing/empty");
        }

        // === Refresh materialized view AFTER all data is loaded ===
        try {
            console.log("[seed] Refreshing mv_product_monthly_stats ...");
            const t0 = Date.now();
            await pool.query("REFRESH MATERIALIZED VIEW mv_product_monthly_stats;");
            const t1 = Date.now();
            console.log(`[seed] mv_product_monthly_stats refreshed in ${t1 - t0} ms`);
        } catch (e) {
            console.error("[seed] Failed to refresh mv_product_monthly_stats:", e.message);
        }

        console.log("[seed] All done. Inspect pgAdmin4 to verify table rows.");
        await pool.end();
        process.exit(0);
    } catch (err) {
        console.error("[seed] Error:", err);
        try {
            await pool.end();
        } catch { }
        process.exit(2);
    }
}

main();
