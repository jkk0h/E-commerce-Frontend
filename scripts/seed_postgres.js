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

// --- CSV loader (simple) ---
function readCSV(file) {
  const raw = fs.readFileSync(file, "utf-8").trim();
  const [header, ...lines] = raw.split(/\r?\n/);
  const cols = header.split(",").map(h => h.trim());
  return lines.filter(Boolean).map(line => {
    const vals = line.split(",").map(x => x.trim());
    const obj = {};
    cols.forEach((c, i) => (obj[c] = vals[i] ?? null));
    return obj;
  });
}

// --- Coercion helpers ---
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

// bulk insert with batching + per-table coercion
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

async function main() {
  // sanity log
  try {
    const u = new URL(POSTGRES_URL);
    console.log("Connecting to Postgres host:", u.hostname, "port:", u.port || "(default)");
  } catch {}

  console.log("Running schema...");
  await runSQL(path.resolve(__dirname, "schema.sql"));

  console.log("Loading product categories...");
  const categoryRows = readCSV(path.join(dataDir, "product_category_name_translation.csv"));
  await bulkInsert(
    "product_category_translation",
    ["product_category_name", "product_category_name_english"],
    categoryRows
  );

  // ------- Option 1: filter products by known categories -------
  console.log("Loading products (filtering unknown categories)...");
  const productsAll = readCSV(path.join(dataDir, "olist_products_dataset.csv"));
  const categorySet = new Set(categoryRows.map(c => c.product_category_name).filter(Boolean));
  const productsFiltered = productsAll.filter(p =>
    !p.product_category_name || categorySet.has(p.product_category_name)
  );
  console.log(`Products total: ${productsAll.length} | inserting: ${productsFiltered.length} | dropped: ${productsAll.length - productsFiltered.length}`);

  await bulkInsert(
    "products",
    ["product_id","product_category_name","product_name_length","product_description_length","product_photos_qty",
     "product_weight_g","product_length_cm","product_height_cm","product_width_cm"],
    productsFiltered,
    { ints: INT_PRODUCTS }
  );

  console.log("Loading sellers...");
  await bulkInsert(
    "sellers",
    ["seller_id","seller_zip_code_prefix","seller_city","seller_state"],
    readCSV(path.join(dataDir, "olist_sellers_dataset.csv")),
    { ints: INT_SELLERS }
  );

  console.log("Loading customers...");
  await bulkInsert(
    "customers",
    ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"],
    readCSV(path.join(dataDir, "olist_customers_dataset.csv")),
    { ints: INT_CUSTOMERS }
  );

  console.log("Loading orders...");
  await bulkInsert(
    "orders",
    ["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at",
     "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"],
    readCSV(path.join(dataDir, "olist_orders_dataset.csv")),
    { dates: DATE_ORDERS }
  );

  console.log("Loading order items...");
  await bulkInsert(
    "order_items",
    ["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"],
    readCSV(path.join(dataDir, "olist_order_items_dataset.csv")),
    { ints: INT_ITEMS, floats: FLOAT_ITEMS, dates: new Set(["shipping_limit_date"]) }
  );

  console.log("Loading order payments...");
  await bulkInsert(
    "order_payments",
    ["order_id","payment_sequential","payment_type","payment_installments","payment_value"],
    readCSV(path.join(dataDir, "olist_order_payments_dataset.csv")),
    { ints: INT_PAYMENTS, floats: FLOAT_PAYMENTS }
  );

  console.log("Loading order reviews...");
  await bulkInsert(
    "order_reviews",
    ["review_id","order_id","review_score","review_comment_title","review_comment_message",
     "review_creation_date","review_answer_timestamp"],
    readCSV(path.join(dataDir, "olist_order_reviews_dataset.csv")),
    { ints: new Set(["review_score"]), dates: DATE_REVIEWS }
  );

  console.log("✅ All CSVs loaded successfully");
  await pool.end();
}

main().catch(e => { console.error("❌ Error during seeding:", e); process.exit(1); });
