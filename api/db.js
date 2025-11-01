// In api/db.js

import { MongoClient } from "mongodb";
import pg from "pg";

export async function connectAll() {
  const pgPool = new pg.Pool({
    connectionString: process.env.POSTGRES_URL,
    ssl: { rejectUnauthorized: false }
  });

  // 👇 CHANGE THIS LINE
  // Use MONGO_URL, which Railway automatically sets, instead of MONGODB_URI
  const mongoClient = new MongoClient(process.env.MONGO_URL); 
  await mongoClient.connect();
  const mongoDb = mongoClient.db(process.env.MONGO_DB_NAME || "appdb");

  return { pgPool, mongoDb, mongoClient };
}
