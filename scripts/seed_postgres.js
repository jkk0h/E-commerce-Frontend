import fs from "fs";
import path from "path";
import pg from "pg";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const POSTGRES_URL = process.env.POSTGRES_URL || process.env.DATABASE_URL;
if (!POSTGRES_URL) {
  console.error("❌ Missing POSTGRES_URL (or DATABASE_URL). Add it in Railway → Scripts service → Variables.");
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: POSTGRES_URL,
  ssl: { rejectUnauthorized: false }
});

// sanity log (password is not printed)
try {
  const u = new URL(POSTGRES_URL);
  console.log("Connecting to Postgres host:", u.hostname, "port:", u.port || "(default)");
} catch {}

function readCSV(file) {
  const raw = fs.readFileSync(file, "utf-8").trim();
  const [headerLine, ...lines] = raw.split(/\r?\n/);
  const headers = headerLine.split(",").map(h => h.trim());
  const rows = lines.filter(Boolean).map(l => {
    const vals = l.split(",").map(x => x.trim());
    const obj = {};
    headers.forEach((h, i) => obj[h] = vals[i] ?? null);
    return obj;
  });
  return rows;
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
    const rowVals = columns.map(c => r[c] ?? null);
    values.push(`(${columns.map(()=>`$${p++}`).join(",")})`);
    params.push(...rowVals);
  }
  const sql = `INSERT INTO ${table} (${columns.join(",")}) VALUES ${values.join(",")}
               ON CONFLICT DO NOTHING`;
  await pool.query(sql, params);
}

async function main() {
  console.log("Running schema...");
  await runSQL(path.join(__dirname, "schema.sql"));

  const dataDir = path.join(__dirname, "..", "data");

  // --- Load product category translations ---
  console.log("Loading product categories...");
  await bulkInsert(
    "product_category_translation",
    ["product_category_name", "product_category_name_english"],
    readCSV(path.join(dataDir, "product_category_name_translation.csv"))
  );

  // --- Load products ---
  console.log("Loading products...");
  await bulkInsert(
    "products",
    ["product_id","product_category_name","product_name_length","product_description_length","product_photos_qty",
     "product_weight_g","product_length_cm","product_height_cm","product_width_cm"],
    readCSV(path.join(dataDir, "olist_products_dataset.csv"))
  );

  // --- Load sellers ---
  console.log("Loading sellers...");
  await bulkInsert(
    "sellers",
    ["seller_id","seller_zip_code_prefix","seller_city","seller_state"],
    readCSV(path.join(dataDir, "olist_sellers_dataset.csv"))
  );

  // --- Load customers ---
  console.log("Loading customers...");
  await bulkInsert(
    "customers",
    ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"],
    readCSV(path.join(dataDir, "olist_customers_dataset.csv"))
  );

  // --- Load orders ---
  console.log("Loading orders...");
  await bulkInsert(
    "orders",
    ["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at",
     "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"],
    readCSV(path.join(dataDir, "olist_orders_dataset.csv"))
  );

  // --- Load order_items ---
  console.log("Loading order items...");
  await bulkInsert(
    "order_items",
    ["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"],
    readCSV(path.join(dataDir, "olist_order_items_dataset.csv"))
  );

  // --- Load order_payments ---
  console.log("Loading order payments...");
  await bulkInsert(
    "order_payments",
    ["order_id","payment_sequential","payment_type","payment_installments","payment_value"],
    readCSV(path.join(dataDir, "olist_order_payments_dataset.csv"))
  );

  // --- Load order_reviews ---
  console.log("Loading order reviews...");
  await bulkInsert(
    "order_reviews",
    ["review_id","order_id","review_score","review_comment_title","review_comment_message",
     "review_creation_date","review_answer_timestamp"],
    readCSV(path.join(dataDir, "olist_order_reviews_dataset.csv"))
  );

  console.log("✅ All CSVs loaded successfully into PostgreSQL!");
  await pool.end();
}

main().catch(err => {
  console.error("❌ Error during seeding:", err);
  process.exit(1);
});
