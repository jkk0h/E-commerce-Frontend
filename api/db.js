import { MongoClient } from "mongodb";
import pg from "pg";

export async function connectAll() {
  const pgPool = new pg.Pool({
    connectionString: process.env.POSTGRES_URL,
    ssl: { rejectUnauthorized: false }
  });

  const mongoClient = new MongoClient(process.env.MONGODB_URI);
  await mongoClient.connect();
  const mongoDb = mongoClient.db(process.env.MONGO_DB_NAME || "appdb");

  return { pgPool, mongoDb, mongoClient };
}
