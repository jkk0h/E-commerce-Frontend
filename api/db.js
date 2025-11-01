// db.js
import { MongoClient } from "mongodb";
import pg from "pg";

export async function connectAll() {
  // --- Postgres (supports multiple env names) ---
  const pgUrl =
    process.env.POSTGRES_URL ||
    process.env.DATABASE_URL ||
    process.env.DATABASE_PUBLIC_URL;

  if (!pgUrl) {
    throw new Error("No Postgres URL found in env (POSTGRES_URL/DATABASE_URL/DATABASE_PUBLIC_URL).");
  }

  const pgPool = new pg.Pool({
    connectionString: pgUrl,
    ssl: { rejectUnauthorized: false },
  });

  // --- Mongo (optional) ---
  let mongoDb = null;
  let mongoClient = null;

  const mongoUri =
    process.env.MONGODB_URI || process.env.MONGO_PUBLIC_URL;

  if (mongoUri) {
    mongoClient = new MongoClient(mongoUri);
    await mongoClient.connect();
    mongoDb = mongoClient.db(process.env.MONGO_DB_NAME || "appdb");
    console.log("Mongo connected");
  } else {
    console.warn("MONGODB_URI/MONGO_PUBLIC_URL not set; starting without Mongo.");
  }

  return { pgPool, mongoDb, mongoClient };
}
