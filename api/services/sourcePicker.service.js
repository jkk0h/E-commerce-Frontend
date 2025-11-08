import { pool } from "../db/pool.js";

/**
 * Check if a table exists
 */
export async function tableExists(schema, table) {
  const sql = `
    SELECT to_regclass($1) IS NOT NULL AS exists
  `;
  const name = `${schema}.${table}`;
  const { rows } = await pool.query(sql, [name]);
  return !!rows[0]?.exists;
}

/**
 * Decide which source to use:
 * - "normalized": if normalized tables exist (order_items_core + order_item_pricing)
 * - "staging": fallback to raw "orders" table
 * - "none": nothing available
 */

export async function pickProductSource() {
  const hasNormalized =
    (await tableExists("public", "order_items_core")) &&
    (await tableExists("public", "order_item_pricing"));

  if (hasNormalized) return "normalized";

  const hasStaging = await tableExists("public", "orders");
  if (hasStaging) return "staging";

  return "none";
}
