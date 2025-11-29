// scripts/seed_mongo.js
// Seed Mongo "orders" collection from raw orders file in scripts/data
//
// Usage:
//   MONGODB_URI="mongodb://localhost:27017" MONGO_DB_NAME="appdb" node scripts/seed_mongo.js
//
// It will look for scripts/data/orders.[csv|xlsx|xls].

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parse as parseCsv } from "csv-parse/sync";
import xlsx from "xlsx";
import { MongoClient } from "mongodb";

// ---------- paths ----------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.resolve(projectRoot, "scripts", "data");

// ---------- env ----------
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://localhost:27017";
const MONGO_DB_NAME = process.env.MONGO_DB_NAME || "appdb";

if (!MONGODB_URI) {
    console.error("❌ Missing MONGODB_URI environment variable.");
    process.exit(1);
}

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
    const candidates = [`${basename}.csv`, `${basename}.xlsx`, `${basename}.xls`];
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
        console.log(`[DATA] Missing: ${basename}.[csv|xlsx|xls] in ${dataDir}`);
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
            for (const k of Object.keys(r)) {
                out[normKey(k)] = r[k] === "" ? null : r[k];
            }
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

async function main() {
    try {
        console.log("Project root:", projectRoot);
        console.log("Data dir:", dataDir);

        if (!fs.existsSync(dataDir)) {
            console.error("❌ Data directory scripts/data does not exist.");
            process.exit(1);
        }

        // 1) Read raw combined orders dataset
        //    Expect file like scripts/data/orders.csv (or xlsx/xls)
        const ordersRaw = readTableFlexible("orders");
        if (!ordersRaw.length) {
            console.error("❌ No orders data found in scripts/data/orders.[csv|xlsx|xls]");
            process.exit(1);
        }

        console.log(`[DATA] Loaded ${ordersRaw.length} rows from orders.*`);

        // 2) Convert types for Mongo
        const docs = ordersRaw.map((r) => {
            const doc = { ...r };

            // numeric conversions…
            if (doc.review_score != null)
                doc.review_score = Number(doc.review_score);

            // ✅ convert timestamps to Date
            if (doc.review_creation_date)
                doc.review_creation_date = new Date(doc.review_creation_date);
            if (doc.review_answer_timestamp)
                doc.review_answer_timestamp = new Date(doc.review_answer_timestamp);
            if (doc.order_purchase_timestamp)
                doc.order_purchase_timestamp = new Date(doc.order_purchase_timestamp);
            if (doc.order_approved_at)
                doc.order_approved_at = new Date(doc.order_approved_at);
            if (doc.order_delivered_carrier_date)
                doc.order_delivered_carrier_date = new Date(doc.order_delivered_carrier_date);
            if (doc.order_delivered_customer_date)
                doc.order_delivered_customer_date = new Date(doc.order_delivered_customer_date);
            if (doc.order_estimated_delivery_date)
                doc.order_estimated_delivery_date = new Date(doc.order_estimated_delivery_date);

            return doc;
        });

        console.log(`[DATA] Prepared ${docs.length} docs for Mongo.`);

        // 3) Connect to Mongo and insert
        console.log("🔗 Connecting to Mongo...");
        const client = new MongoClient(MONGODB_URI);
        await client.connect();
        const db = client.db(MONGO_DB_NAME);
        const coll = db.collection("orders");

        console.log("🧹 Clearing existing Mongo 'orders' collection...");
        await coll.deleteMany({});

        if (!docs.length) {
            console.log("No documents to insert. Exiting.");
            await client.close();
            process.exit(0);
        }

        const batchSize = 5000;
        let total = 0;
        for (let i = 0; i < docs.length; i += batchSize) {
            const chunk = docs.slice(i, i + batchSize);
            await coll.insertMany(chunk);
            total += chunk.length;
            console.log(`Inserted ${total}/${docs.length} into Mongo.orders...`);
        }

        console.log(`🎉 Done. Inserted ${total} documents into Mongo 'orders' collection.`);
        await client.close();
        process.exit(0);
    } catch (err) {
        console.error("❌ Error seeding Mongo from raw orders:", err);
        process.exit(1);
    }
}

main();
