import { pool } from "../db/pool.js";

export async function getProducts(search, limit, offset, src) {
  const q = (search || "").trim();

  if (src === "normalized") {
    const sql = `
      SELECT oic.product_id AS id,
             ROUND(AVG(oip.price)::numeric, 2) AS price,
             COUNT(*) AS order_count
      FROM order_items_core oic
      JOIN order_item_pricing oip
        ON oip.order_id=oic.order_id AND oip.order_item_id=oic.order_item_id
      WHERE ($1 = '' OR oic.product_id ILIKE '%' || $1 || '%')
      GROUP BY oic.product_id
      ORDER BY order_count DESC
      LIMIT $2 OFFSET $3
    `;
    const { rows } = await pool.query(sql, [q, limit, offset]);
    return rows;
  }

  if (src === "staging") {
    const sql = `
      SELECT product_id AS id,
             ROUND(AVG(price)::numeric, 2) AS price,
             COUNT(*) AS order_count
      FROM orders
      WHERE product_id IS NOT NULL
        AND ($1 = '' OR product_id ILIKE '%' || $1 || '%')
      GROUP BY product_id
      ORDER BY order_count DESC
      LIMIT $2 OFFSET $3
    `;
    const { rows } = await pool.query(sql, [q, limit, offset]);
    return rows;
  }

  return [];
}

export async function getProductById(id, src) {
  if (src === "normalized") {
    const sql = `
      SELECT oic.product_id AS id,
             ROUND(AVG(oip.price)::numeric, 2) AS price,
             COUNT(*) AS order_count
      FROM order_items_core oic
      JOIN order_item_pricing oip
        ON oip.order_id=oic.order_id AND oip.order_item_id=oic.order_item_id
      WHERE oic.product_id=$1
      GROUP BY oic.product_id
    `;
    const { rows } = await pool.query(sql, [id]);
    return rows[0] || null;
  }

  if (src === "staging") {
    const sql = `
      SELECT product_id AS id,
             ROUND(AVG(price)::numeric, 2) AS price,
             COUNT(*) AS order_count
      FROM orders
      WHERE product_id=$1
      GROUP BY product_id
    `;
    const { rows } = await pool.query(sql, [id]);
    return rows[0] || null;
  }

  return null;
}
