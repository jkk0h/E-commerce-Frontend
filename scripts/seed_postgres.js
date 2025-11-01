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

// data dir is sibling at repo root: /data
const dataDir = path.resolve(__dirname, "/data");
console.log("Using dataDir:", dataDir);  // sanity log

// tiny csv loader
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

async function runSQL(sqlPath) {
  const sql = fs.readFileSync(sqlPath, "utf-8");
  await pool.query(sql);
}

async function bulkInsert(table, columns, rows) {
  if (!rows.length) return;
  const values = [];
  const params = [];
  let p = 1;
  for (const r of rows) {
    values.push(`(${columns.map(() => `$${p++}`).join(",")})`);
    params.push(...columns.map(c => r[c] ?? null));
  }
  const sql = `INSERT INTO ${table} (${columns.join(",")}) VALUES ${values.join(",")}
               ON CONFLICT DO NOTHING`;
  await pool.query(sql, params);
}

async function main() {
  // optional: log host sanity
  try {
    const u = new URL(POSTGRES_URL);
    console.log("Connecting to Postgres host:", u.hostname, "port:", u.port || "(default)");
  } catch {}

  console.log("Running schema...");
  await runSQL(path.resolve(__dirname, "schema.sql"));

  console.log("Loading product categories...");
  await bulkInsert(
    "product_category_translation",
    ["product_category_name", "product_category_name_english"],
    readCSV(path.join(dataDir, "product_category_name_translation.csv"))
  );

  console.log("Loading products...");
  await bulkInsert(
    "products",
    ["product_id","product_category_name","product_name_length","product_description_length","product_photos_qty",
     "product_weight_g","product_length_cm","product_height_cm","product_width_cm"],
    readCSV(path.join(dataDir, "olist_products_dataset.csv"))
  );

  console.log("Loading sellers...");
  await bulkInsert(
    "sellers",
    ["seller_id","seller_zip_code_prefix","seller_city","seller_state"],
    readCSV(path.join(dataDir, "olist_sellers_dataset.csv"))
  );

  console.log("Loading customers...");
  await bulkInsert(
    "customers",
    ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"],
    readCSV(path.join(dataDir, "olist_customers_dataset.csv"))
  );

  console.log("Loading orders...");
  await bulkInsert(
    "orders",
    ["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at",
     "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"],
    readCSV(path.join(dataDir, "olist_orders_dataset.csv"))
  );

  console.log("Loading order items...");
  await bulkInsert(
    "order_items",
    ["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"],
    readCSV(path.join(dataDir, "olist_order_items_dataset.csv"))
  );

  console.log("Loading order payments...");
  await bulkInsert(
    "order_payments",
    ["order_id","payment_sequential","payment_type","payment_installments","payment_value"],
    readCSV(path.join(dataDir, "olist_order_payments_dataset.csv"))
  );

  console.log("Loading order reviews...");
  await bulkInsert(
    "order_reviews",
    ["review_id","order_id","review_score","review_comment_title","review_comment_message",
     "review_creation_date","review_answer_timestamp"],
    readCSV(path.join(dataDir, "olist_order_reviews_dataset.csv"))
  );

  console.log("✅ All CSVs loaded successfully");
  await pool.end();
}

main().catch(e => { console.error("❌ Error during seeding:", e); process.exit(1); });
