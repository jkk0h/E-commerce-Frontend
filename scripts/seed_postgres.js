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

/* ---------------- CSV utils: normalize header once ---------------- */

// strip BOM, outer quotes, lowercase, trim, spaces->underscore
function normKey(s) {
  return (s || "")
    .replace(/^\uFEFF/, "")      // strip BOM
    .replace(/^"+|"+$/g, "")     // strip wrapping double quotes
    .toLowerCase()
    .trim()
    .replace(/\s+/g, "_");       // collapse spaces -> underscore
}

// split, trim, and de-quote each cell (Olist has no quoted commas)
function splitCSVLine(line) {
  return line
    .split(",")
    .map(s => s.trim().replace(/^"+|"+$/g, "")); // dequote cell values
}

function readCSV(file) {
  const raw = fs.readFileSync(file, "utf-8");
  const lines = raw.split(/\r?\n/).filter(l => l.trim().length); // drop blanks
  if (!lines.length) return [];

  // normalize header to stable keys
  const headerLine = lines[0].replace(/^\uFEFF/, "");
  const cols = splitCSVLine(headerLine).map(normKey);

  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = splitCSVLine(lines[i]);
    const row = {};
    for (let c = 0; c < cols.length; c++) {
      row[cols[c]] = (c < vals.length ? vals[c] : null);
    }
    out.push(row);
  }
  return out;
}

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

  // sellers
  console.log("Loading sellers...");
  const sellersAll = readCSV(path.join(dataDir, "olist_sellers_dataset.csv"));
  await bulkInsert(
    "sellers",
    ["seller_id","seller_zip_code_prefix","seller_city","seller_state"],
    sellersAll,
    { ints: INT_SELLERS }
  );

  // customers
  console.log("Loading customers...");
  const customersAll = readCSV(path.join(dataDir, "olist_customers_dataset.csv"));
  console.log("Sample customer keys:", Object.keys(customersAll[0] || {}));
  const customersFiltered = customersAll
    .filter(r => r.customer_id && r.customer_id.trim().length)
    .map(r => ({
      customer_id: r.customer_id,
      customer_unique_id: r.customer_unique_id ?? null,
      customer_zip_code_prefix: r.customer_zip_code_prefix ?? null,
      customer_city: r.customer_city ?? null,
      customer_state: r.customer_state ?? null,
    }));
  console.log(`Customers total: ${customersAll.length} | inserting: ${customersFiltered.length} | dropped: ${customersAll.length - customersFiltered.length}`);
  await bulkInsert(
    "customers",
    ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"],
    customersFiltered,
    { ints: INT_CUSTOMERS }
  );

  // orders (skip blank order_id rows)
  console.log("Loading orders...");
  const ordersAll = readCSV(path.join(dataDir, "olist_orders_dataset.csv"));
  console.log("Sample order keys:", Object.keys(ordersAll[0] || {}));
  const ordersFiltered = ordersAll
    .filter(o => o.order_id && o.order_id.trim().length)
    .map(o => ({
      order_id: o.order_id,
      customer_id: o.customer_id ?? null,
      order_status: o.order_status ?? null,
      order_purchase_timestamp: o.order_purchase_timestamp ?? null,
      order_approved_at: o.order_approved_at ?? null,
      order_delivered_carrier_date: o.order_delivered_carrier_date ?? null,
      order_delivered_customer_date: o.order_delivered_customer_date ?? null,
      order_estimated_delivery_date: o.order_estimated_delivery_date ?? null,
    }));
  console.log(`Orders total: ${ordersAll.length} | inserting: ${ordersFiltered.length} | dropped: ${ordersAll.length - ordersFiltered.length}`);
  await bulkInsert(
    "orders",
    ["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at",
     "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"],
    ordersFiltered,
    { dates: DATE_ORDERS }
  );

// order items — pre-filter to existing orders/products/sellers to avoid FK errors
console.log("Loading order items...");
const itemsAll = readCSV(path.join(dataDir, "olist_order_items_dataset.csv"));

// Build existence sets from what we actually inserted above
const orderIdSet   = new Set(ordersFiltered.map(o => o.order_id));
const productIdSet = new Set(productsFiltered.map(p => p.product_id));
// We didn't filter sellers, so all were inserted:
const sellerIdSet  = new Set(sellersAll.map(s => s.seller_id));

// Keep only valid foreign-key rows
const itemsFiltered = itemsAll.filter(r =>
  r.order_id && orderIdSet.has(r.order_id) &&
  r.product_id && productIdSet.has(r.product_id) &&
  r.seller_id && sellerIdSet.has(r.seller_id)
);

const droppedItems = itemsAll.length - itemsFiltered.length;
console.log(`Order items total: ${itemsAll.length} | inserting: ${itemsFiltered.length} | dropped (FK-miss): ${droppedItems}`);

await bulkInsert(
  "order_items",
  ["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"],
  itemsFiltered,
  { ints: INT_ITEMS, floats: FLOAT_ITEMS, dates: new Set(["shipping_limit_date"]) }
);


  // payments
  console.log("Loading order payments...");
  const paysAll = readCSV(path.join(dataDir, "olist_order_payments_dataset.csv"));
  await bulkInsert(
    "order_payments",
    ["order_id","payment_sequential","payment_type","payment_installments","payment_value"],
    paysAll,
    { ints: INT_PAYMENTS, floats: FLOAT_PAYMENTS }
  );

// order reviews — keep rows that reference existing orders, have a review_id,
// coerce dates, and ensure score is 1..5
console.log("Loading order reviews...");
const reviewsAll = readCSV(path.join(dataDir, "olist_order_reviews_dataset.csv"));

// Build existence set from actually-inserted orders
const orderIdSet = new Set(ordersFiltered.map(o => o.order_id));

// Keep only valid rows
const reviewsFiltered = reviewsAll.filter(r =>
  r.review_id && r.review_id.trim().length &&
  r.order_id && orderIdSet.has(r.order_id)
).map(r => {
  // normalise dates: "YYYY-MM-DD hh:mm:ss" -> "YYYY-MM-DDThh:mm:ss"
  const fixDate = (s) => {
    if (!s || !s.trim()) return null;
    const t = s.includes(" ") ? s.replace(" ", "T") : s;
    const d = new Date(t);
    return Number.isNaN(d.getTime()) ? null : d;
  };

  // normalise score: must be 1..5 or null (to avoid constraint errors)
  const scoreNum = r.review_score ? parseInt(r.review_score, 10) : null;
  const score = (Number.isInteger(scoreNum) && scoreNum >= 1 && scoreNum <= 5) ? scoreNum : null;

  return {
    review_id: r.review_id,
    order_id: r.order_id,
    review_score: score,
    review_comment_title: r.review_comment_title ?? null,
    review_comment_message: r.review_comment_message ?? null,
    review_creation_date: fixDate(r.review_creation_date),
    review_answer_timestamp: fixDate(r.review_answer_timestamp)
  };
});

const droppedReviews = reviewsAll.length - reviewsFiltered.length;
console.log(`Order reviews total: ${reviewsAll.length} | inserting: ${reviewsFiltered.length} | dropped (FK/invalid): ${droppedReviews}`);

await bulkInsert(
  "order_reviews",
  ["review_id","order_id","review_score","review_comment_title","review_comment_message",
   "review_creation_date","review_answer_timestamp"],
  reviewsFiltered,
  { ints: new Set(["review_score"]), dates: new Set(["review_creation_date","review_answer_timestamp"]) }
);

  console.log("✅ All CSVs loaded successfully");
  await pool.end();
}

main().catch(e => { console.error("❌ Error during seeding:", e); process.exit(1); });
