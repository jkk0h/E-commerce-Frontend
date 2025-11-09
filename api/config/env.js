import dotenv from "dotenv";
dotenv.config();

export const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
export const POSTGRES_URL = process.env.POSTGRES_URL || process.env.DATABASE_URL;

if (!POSTGRES_URL) {
  console.error("❌ Missing POSTGRES_URL/DATABASE_URL in environment.");
  process.exit(1);
}

export function shouldUseSsl() {
  // common pattern for Render/Railway/Heroku
  const force = process.env.PGSSL?.toLowerCase() === "true";
  const isProd = process.env.NODE_ENV === "production";
  return force || isProd;
}

// dev guard for the raw-SQL route
export const ALLOW_SQL = (process.env.ALLOW_SQL || "false").toLowerCase() === "true";
