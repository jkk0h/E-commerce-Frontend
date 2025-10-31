import { MongoClient } from "mongodb";

const client = new MongoClient(process.env.MONGODB_URI);
await client.connect();
const db = client.db(process.env.MONGO_DB_NAME || "appdb");

await db.collection("products").deleteMany({});
await db.collection("products").insertMany([
  { _id: "PV_1001", sku: "SLUSHi-SAGE-500", title: "Ninja SLUSHi – Sage 500ml", price: 129.9, active: true },
  { _id: "PV_1002", sku: "CRISPi-STONE-4L", title: "Ninja CRISPi – Stone 4L", price: 169.9, active: true }
]);

console.log("Seeded Mongo ✅");
await client.close();
