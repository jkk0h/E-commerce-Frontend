import fs from "fs";
import path from "path";
import pg from "pg";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// require POSTGRES_URL
const POSTGRES_URL = process.env.POSTGRES_URL || process.env.DATABASE_URL;
if (!POSTGRES_URL) {
  console.error("❌ Missing POSTGRES_URL.");
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: POSTGRES_URL,
  ssl: { rejectUnauthorized: false }
});

// Option B layout: CSVs live at /scripts/data
const dataDir = path.resolve(__dirname, "data");

/* ---------------- CSV utils (BOM-safe) ---------------- */

function splitCSVLine(line) {
  // simple split for this dataset (no embedded commas/quotes in Olist)
  return line.split(",").map(s => s.trim());
}

function readCSV(file) {
  const raw = fs.readFileSync(file, "utf-8").trim();
  const lines = raw.split(/\r?\n/).filter(l => l.trim().length); // drop blanks
  if (!lines.length) return [];

  // strip BOM from the first line, and from each column name just in case
  const headerLine = lines[0].replace(/^\uFEFF/, "");
  const cols = splitCSVLine(headerLine).map(h => h.replace(/^\uFEFF/, ""));

  const rows = lines.slice(1);
  return rows.map(line => {
    const vals = splitCSVLine(line);
    const obj = {};
    cols.forEach((c, i) => (obj[c] = (i < vals.length ? vals[i] : null)));
    return obj;
  });
}

// helper: get a value by key, tolerant to BOM'd key names
const v = (obj, key) => obj[key] ?? obj["\uFEFF" + key] ?? null;

/* ---------------- Coercion helpers ---------------- */

const INT_PRODUCTS = new Set([
  "product_name_length","product_description_length","product_photos_qty",
  "product_weight_g","product_length_cm","product_height_cm","product_width_cm"
]);

const INT_SELLERS = new Set(["seller_zip_code_prefix"]);
const INT_CUSTOMERS = new Set(["customer_zip_code_prefix"]);

const INT_ITEMS = new Set(["order_item_id"]);
const FLOAT_ITEMS = new Set(["price","freight_value"]);

const INT_PAYMENTS = new Set(["payment_sequential","payment_installments"]);
const FLOAT_PAYMENTS = new Set(["payment_value"]);

const DATE_ORDERS = new Set([
  "order_purchase_timestamp","order_approved_at",
  "order_delivered_carrier_date","order_delivered_customer_date",
  "order_estimated_delivery_date"
]);

const DATE_REVIEWS = new Set(["review_creation_date","review_answer_timestamp"]);

function coerceValue(col, val, {ints = new Set(), floats = new Set(), dates = new Set()} = {}) {
  if (val === "" || val === undefined || val === null) return null;
  if (ints.has(col))  { const n = parseInt(val, 10);  return Number.isNaN(n) ? null : n; }
  if (floats.has(col)){ const f = parseFloat(val);     return Number.isNaN(f) ? null : f; }
  if (dates.has(col)) { const d = new Date(val);       return Number.isNaN(d.getTime()) ? null : d; }
  return val;
}

/* ---------------- DB helpers ---------------- */

async function bulkInsert(table, columns, rows, opts = {}, batchSize = 500) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += batchSize) {
    const chunk = rows.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const r of chunk) {
      values.push(`(${columns.map(() => `$${p++}`).join(",")})`);
      params.push(...columns.map(c => coerceValue(c, r[c], opts)));
    }
    const sql = `
      INSERT INTO ${table} (${columns.join(",")})
      VALUES ${values.join(",")}
      ON CONFLICT DO NOTHING;
    `;
    await pool.query(sql, params);
    console.log(`Inserted ${Math.min(i + batchSize, rows.length)}/${rows.length} rows into ${table}`);
  }
}

async function runSQL(sqlPath) {
  const sql = fs.readFileSync(sqlPath, "utf-8");
  await pool.query(sql);
}

/* ---------------- main ---------------- */

async function main() {
  try {
    const u = new URL(POSTGRES_URL);
    console.log("Connecting to Postgres host:", u.hostname, "port:", u.port || "(default)");
  } catch {}

  console.log("Running schema...");
  await runSQL(path.resolve(__dirname, "schema.sql"));

  // product categories
  console.log("Loading product categories...");
  const categoryRows = readCSV(path.join(dataDir, "product_category_name_translation.csv"));
  await bulkInsert(
    "product_category_translation",
    ["product_category_name", "product_category_name_english"],
    categoryRows
  );

  // products (Option 1: filter by known categories)
  console.log("Loading products (filtering unknown categories)...");
  const productsAll = readCSV(path.join(dataDir, "olist_products_dataset.csv"));
  const categorySet = new Set(categoryRows.map(c => v(c, "product_category_name")).filter(Boolean));
  const productsFiltered = productsAll.filter(p =>
    !v(p, "product_category_name") || categorySet.has(v(p, "product_category_name"))
  );
  console.log(`Products total: ${productsAll.length} | inserting: ${productsFiltered.length} | dropped: ${productsAll.length - productsFiltered.length}`);

  await bulkInsert(
    "products",
    ["product_id","product_category_name","product_name_length","product_description_length","product_photos_qty",
     "product_weight_g","product_length_cm","product_height_cm","product_width_cm"],
    productsFiltered.map(p => ({
      product_id: v(p, "product_id"),
      product_category_name: v(p, "product_category_name"),
      product_name_length: v(p, "product_name_length"),
      product_description_length: v(p, "product_description_length"),
      product_photos_qty: v(p, "product_photos_qty"),
      product_weight_g: v(p, "product_weight_g"),
      product_length_cm: v(p, "product_length_cm"),
      product_height_cm: v(p, "product_height_cm"),
      product_width_cm: v(p, "product_width_cm"),
    })),
    { ints: INT_PRODUCTS }
  );

  // sellers
  console.log("Loading sellers...");
  const sellersAll = readCSV(path.join(dataDir, "olist_sellers_dataset.csv"));
  await bulkInsert(
    "sellers",
    ["seller_id","seller_zip_code_prefix","seller_city","seller_state"],
    sellersAll.map(s => ({
      seller_id: v(s, "seller_id"),
      seller_zip_code_prefix: v(s, "seller_zip_code_prefix"),
      seller_city: v(s, "seller_city"),
      seller_state: v(s, "seller_state"),
    })),
    { ints: INT_SELLERS }
  );

  // customers (BOM-safe + filter rows with missing customer_id)
  console.log("Loading customers...");
  const customersAll = readCSV(path.join(dataDir, "olist_customers_dataset.csv"));
  const customersFiltered = customersAll
    .map(r => ({
      customer_id: v(r, "customer_id"),
      customer_unique_id: v(r, "customer_unique_id"),
      customer_zip_code_prefix: v(r, "customer_zip_code_prefix"),
      customer_city: v(r, "customer_city"),
      customer_state: v(r, "customer_state"),
    }))
    .filter(r => r.customer_id && r.customer_id.trim().length);

  console.log(`Customers total: ${customersAll.length} | inserting: ${customersFiltered.length} | dropped: ${customersAll.length - customersFiltered.length}`);

  await bulkInsert(
    "customers",
    ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"],
    customersFiltered,
    { ints: INT_CUSTOMERS }
  );

  // orders
  console.log("Loading orders...");
  const ordersAll = readCSV(path.join(dataDir, "olist_orders_dataset.csv"));
  await bulkInsert(
    "orders",
    ["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at",
     "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"],
    ordersAll.map(o => ({
      order_id: v(o, "order_id"),
      customer_id: v(o, "customer_id"),
      order_status: v(o, "order_status"),
      order_purchase_timestamp: v(o, "order_purchase_timestamp"),
      order_approved_at: v(o, "order_approved_at"),
      order_delivered_carrier_date: v(o, "order_delivered_carrier_date"),
      order_delivered_customer_date: v(o, "order_delivered_customer_date"),
      order_estimated_delivery_date: v(o, "order_estimated_delivery_date"),
    })),
    { dates: DATE_ORDERS }
  );

  // order items
  console.log("Loading order items...");
  const itemsAll = readCSV(path.join(dataDir, "olist_order_items_dataset.csv"));
  await bulkInsert(
    "order_items",
    ["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"],
    itemsAll.map(i => ({
      order_id: v(i, "order_id"),
      order_item_id: v(i, "order_item_id"),
      product_id: v(i, "product_id"),
      seller_id: v(i, "seller_id"),
      shipping_limit_date: v(i, "shipping_limit_date"),
      price: v(i, "price"),
      freight_value: v(i, "freight_value"),
    })),
    { ints: INT_ITEMS, floats: FLOAT_ITEMS, dates: new Set(["shipping_limit_date"]) }
  );

  // payments
  console.log("Loading order payments...");
  const paysAll = readCSV(path.join(dataDir, "olist_order_payments_dataset.csv"));
  await bulkInsert(
    "order_payments",
    ["order_id","payment_sequential","payment_type","payment_installments","payment_value"],
    paysAll.map(p => ({
      order_id: v(p, "order_id"),
      payment_sequential: v(p, "payment_sequential"),
      payment_type: v(p, "payment_type"),
      payment_installments: v(p, "payment_installments"),
      payment_value: v(p, "payment_value"),
    })),
    { ints: INT_PAYMENTS, floats: FLOAT_PAYMENTS }
  );

  // reviews
  console.log("Loading order reviews...");
  const reviewsAll = readCSV(path.join(dataDir, "olist_order_reviews_dataset.csv"));
  await bulkInsert(
    "order_reviews",
    ["review_id","order_id","review_score","review_comment_title","review_comment_message",
     "review_creation_date","review_answer_timestamp"],
    reviewsAll.map(r => ({
      review_id: v(r, "review_id"),
      order_id: v(r, "order_id"),
      review_score: v(r, "review_score"),
      review_comment_title: v(r, "review_comment_title"),
      review_comment_message: v(r, "review_comment_message"),
      review_creation_date: v(r, "review_creation_date"),
      review_answer_timestamp: v(r, "review_answer_timestamp"),
    })),
    { ints: new Set(["review_score"]), dates: DATE_REVIEWS }
  );

  console.log("✅ All CSVs loaded successfully");
  await pool.end();
}

main().catch(e => { console.error("❌ Error during seeding:", e); process.exit(1); });
