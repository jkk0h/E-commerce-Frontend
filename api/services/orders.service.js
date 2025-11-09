import { pool } from "../db/pool.js";
import { newOrderId } from "../utils/id.js";

/**
 * Creates an order with items.
 * items: [{ product_id: string, qty: number, price?: number }]
 * If price is omitted, we try to infer the avg price from history (best-effort).
 */

export async function createOrder({ customer = "guest", items = [] }) {
  if (!Array.isArray(items) || items.length === 0) {
    const e = new Error("At least one item is required");
    e.statusCode = 400;
    throw e;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const orderId = newOrderId();

    await client.query(
      `INSERT INTO orders_core (order_id, customer_ref)
       VALUES ($1, $2)`,
      [orderId, customer]
    );

    for (const [idx, it] of items.entries()) {
      const pid = String(it.product_id || "").trim();
      const qty = Number(it.qty || 0);
      if (!pid || !Number.isFinite(qty) || qty <= 0) {
        const e = new Error(`Invalid item at index ${idx}`);
        e.statusCode = 400;
        throw e;
      }

      // Insert order item
      await client.query(
        `INSERT INTO order_items_core (order_id, order_item_id, product_id, quantity)
         VALUES ($1, $2, $3, $4)`,
        [orderId, idx + 1, pid, qty]
      );

      // Determine price: given or inferred
      let price = it.price;
      if (price === undefined || price === null) {
        const { rows } = await client.query(
          `SELECT ROUND(AVG(price)::numeric, 2) AS price
             FROM order_item_pricing oip
             JOIN order_items_core oic
               ON oip.order_id=oic.order_id AND oip.order_item_id=oic.order_item_id
            WHERE oic.product_id=$1`,
          [pid]
        );
        price = rows[0]?.price ?? null;
      }

      await client.query(
        `INSERT INTO order_item_pricing (order_id, order_item_id, price)
         VALUES ($1, $2, $3)`,
        [orderId, idx + 1, price]
      );
    }

    await client.query("COMMIT");
    return { order_id: orderId, item_count: items.length };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
