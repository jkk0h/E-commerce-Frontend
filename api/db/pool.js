import pg from "pg";
import { POSTGRES_URL, shouldUseSsl } from "../config/env.js";

export const pool = new pg.Pool({
  connectionString: POSTGRES_URL,
  ssl: shouldUseSsl() ? { rejectUnauthorized: false } : false,
});
