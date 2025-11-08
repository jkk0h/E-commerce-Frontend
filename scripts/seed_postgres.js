// scripts/seed_postgres.js
// Usage:
//   POSTGRES_URL="postgres://user:pass@host:5432/dbname" node scripts/seed_postgres.js
//
// Finds data/ + schema.sql robustly (whether running from /app or /app/scripts)
// and seeds the normalized schema with the CSVs.

import fs from "fs";
import path from "path";
import pg from "pg";
import { parse } from "csv-parse/sync";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/* ---------------- Path resolution (robust) ---------------- */
function firstExisting(paths) {
  for (const p of paths) {
    try { if (p && fs.existsSync(p)) return p; } catch {}
  }
  return null;
}

// data directory can be /app/data (if script at root) OR /app/scripts/data (your repo)
const dataDir = firstExisting([
  path.resolve(__dirname, "data"),
  path.resolve(__dirname, "scripts/data"),
]);

// schema can be /app/schema.sql OR /app/scripts/schema.sql
const schemaPath = firstExisting([
  path.resolve(__dirname, "schema.sql"),
  path.resolve(__dirname, "scripts/schema.sql"),
]);

console.log("Data dir:", dataDir || "(not found)");
console.log("Schema: ", schemaPath || "(not found)");

if (!dataDir) {
  console.error("❌ Data directory not found. Put CSVs in /scripts/data in your repo.");
  process.exit(1);
}

/* ---------------- PG connection (Railway-safe SSL) ---------------- */
const RAW_DB_URL =
  process.env.POSTGRES_URL ||
  process.env.DATABASE_URL ||
  process.env.DATABASE_PUBLIC_URL;

if (!RAW_DB_URL) {
  console.error("❌ Missing POSTGRES_URL / DATABASE_URL / DATABASE_PUBLIC_URL.");
  process.exit(1);
}

// Ensure sslmode=require in the URL
const withSSLParam = RAW_DB_URL.includes("sslmode=")
  ? RAW_DB_URL
  : RAW_DB_URL + (RAW_DB_URL.includes("?") ? "&" : "?") + "sslmode=require";

// Some builders set stricter TLS; make sure pg uses SSL without rejecting the proxy cert.
process.env.PGSSLMODE = process.env.PGSSLMODE || "require";

const pool = new pg.Pool({
  connectionString: withSSLParam,
  ssl: { rejectUnauthorized: false },
});

/* ---------------- CSV helpers ---------------- */
function normKey(s = "") {
  return String(s || "")
    .replace(/^\uFEFF/, "")   // BOM
    .replace(/^"+|"+$/g, "")  // strip wrapping quotes
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

function readCsvFlexible(filename) {
  const full = path.join(dataDir, filename);
  if (!fs.existsSync(full)) {
    console.log(`[CSV] Missing: ${filename}`);
    return [];
  }
  const raw = fs.readFileSync(full, "utf8");
  const firstLine = raw.split(/\r?\n/, 2)[0] || "";
  const delimiter = detectDelimiter(firstLine);
  const rows = parse(raw, { columns: true, skip_empty_lines: true, relax_column_count: true, delimiter });
  return rows.map(r => {
    const out = {};
    for (const k of Object.keys(r)) {
      out[normKey(k)] = r[k] === "" ? null : r[k];
    }
    return out;
  });
}

/* ---------------- DDL runner ---------------- */
async function runDDL() {
  if (!schemaPath) {
    console.log("[DDL] schema.sql not found; skipping DDL.");
    return;
  }
  let sql = fs.readFileSync(schemaPath, "utf8");
  // remove COPY / \copy sections (we load from ./data instead)
  sql = sql.replace(/^COPY[\s\S]*?FROM\s+'\/tmp[\s\S]*?;\s*$/gmi, "");
  sql = sql.replace(/^\\copy[\s\S]*?;\s*$/gmi, "");
  console.log("[DDL] Running schema.sql ...");
  await pool.query(sql);
  console.log("[DDL] DDL complete.");
}

/* ---------------- DB insert batching ---------------- */
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
        if (opts.ints && opts.ints.has(c))   v = v == null ? null : parseInt(v, 10);
        if (opts.floats && opts.floats.has(c)) v = v == null ? null : parseFloat(v);
        if (opts.dates && opts.dates.has(c) && v != null) {
          const d = new Date(v);
          v = Number.isNaN(d.getTime()) ? null : d.toISOString();
        }
        params.push(v == null ? null : v);
      }
    }
    const sql = `INSERT INTO ${table} (${cols.join(",")}) VALUES ${placeholders.join(",")} ON CONFLICT DO NOTHING;`;
    await pool.query(sql, params);
    console.log(`[DB] ${table}: inserted ${Math.min(i + batchSize, objs.length)}/${objs.length}`);
  }
  return objs.length;
}

/* ---------------- Main seeding flow ---------------- */
async function main() {
  try {
    console.log("Resolved data dir:", dataDir);
    console.log("Resolved schema:", schemaPath || "(not found)");

    await runDDL();

    // --- Map CSV filenames you provided ---
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
      orders_combined: "orders.csv", // optional fallback
    };

    const orderItemCore      = readCsvFlexible(csvFiles.order_item_core);
    const orderItemPricing   = readCsvFlexible(csvFiles.order_item_pricing);
    const orderItemShipping  = readCsvFlexible(csvFiles.order_item_shipping);
    const orderPaymentAmount = readCsvFlexible(csvFiles.order_payment_amount);
    const orderPaymentMethod = readCsvFlexible(csvFiles.order_payment_method);
    const orderReviewCore    = readCsvFlexible(csvFiles.order_review_core);
    const orderReviewText    = readCsvFlexible(csvFiles.order_review_text);
    const ordersHeader       = readCsvFlexible(csvFiles.orders_header);
    const ordersTimestamps   = readCsvFlexible(csvFiles.orders_timestamps);
    const ordersCombined     = readCsvFlexible(csvFiles.orders_combined);

    // 1) payment_type_dim
    if (orderPaymentMethod.length) {
      const types = [...new Set(orderPaymentMethod.map(p => (p.payment_type || "").trim()).filter(Boolean))];
      if (types.length) {
        await insertBatch("payment_type_dim", ["payment_type"], types.map(t => ({ payment_type: t })));
        console.log("[seed] payment_type_dim seeded");
      }
    } else {
      console.log("[seed] order_payment_method.csv missing/empty");
    }

    // 2) orders_header
    if (ordersHeader.length) {
      const headers = ordersHeader.map(r => ({
        order_id: r.order_id,
        customer_id: r.customer_id ?? null,
        order_status: r.order_status ?? null
      }));
      await insertBatch("orders_header", ["order_id", "customer_id", "order_status"], headers);
      console.log("[seed] orders_header inserted");
    } else if (ordersCombined.length) {
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

    // 3) orders_timestamps
    {
      const tsCols = ["order_id", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"];
      if (ordersTimestamps.length) {
        const rows = ordersTimestamps.map(r => Object.fromEntries(tsCols.map(c => [c, r[c] ?? null])));
        await insertBatch("orders_timestamps", tsCols, rows, { dates: new Set(tsCols.slice(1)) });
        console.log("[seed] orders_timestamps inserted");
      } else if (ordersCombined.length) {
        const rows = ordersCombined.map(r => Object.fromEntries(tsCols.map(c => [c, r[c] ?? null]))).filter(x => x.order_id);
        if (rows.length) {
          await insertBatch("orders_timestamps", tsCols, rows, { dates: new Set(tsCols.slice(1)) });
          console.log("[seed] orders_timestamps (from orders.csv) inserted");
        }
      } else {
        console.log("[seed] orders_timestamps missing and no fallback");
      }
    }

    // 4) items: core + pricing + shipping
    if (orderItemCore.length) {
      const coreCols = ["order_id", "order_item_id", "product_id", "seller_id"];
      const coreRows = orderItemCore.map(r => ({
        order_id: r.order_id,
        order_item_id: r.order_item_id,
        product_id: r.product_id,
        seller_id: r.seller_id ?? null
      }));
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

    // 5) payments: method + amount
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

    // 6) reviews: core + text
    if (orderReviewCore.length) {
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

    console.log("[seed] All done. Verify tables and row counts in Railway’s SQL editor.");
    await pool.end();
    process.exit(0);
  } catch (err) {
    console.error("[seed] Error:", err);
    try { await pool.end(); } catch {}
    process.exit(2);
  }
}

main();
