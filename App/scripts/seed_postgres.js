import fs from "fs";
import path from "path";
import pg from "pg";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const pool = new pg.Pool({
  connectionString: process.env.POSTGRES_URL,
  ssl: { rejectUnauthorized: false }
});

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

  console.log("Loading dimensions...");
  await bulkInsert("dates", ["date_id","year_no","month_no","day_no"], readCSV(path.join(dataDir,"dates.csv")));
  await bulkInsert("customers", ["customer_id","customer_name","created_at"], readCSV(path.join(dataDir,"customers.csv")));
  await bulkInsert("styles", ["style_id","style_name"], readCSV(path.join(dataDir,"styles.csv")));
  await bulkInsert("sizes", ["size_id","size_label"], readCSV(path.join(dataDir,"sizes.csv")));
  await bulkInsert("product_variants", ["product_variant_id","sku","style_id","size_id"], readCSV(path.join(dataDir,"product_variants.csv")));

  console.log("Loading fact sales...");
  await bulkInsert("sales", ["date_id","customer_id","product_variant_id","pcs","rate","gross_amt"], readCSV(path.join(dataDir,"sales.csv")));

  console.log("Done seeding Postgres ✅");
  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
