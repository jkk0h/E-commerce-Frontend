// normalize DB rows into your frontend-friendly product shape
export function toUiProduct(row) {
  return {
    id: row.id,
    sku: row.id,
    title: row.id,
    name: row.id,
    price: row.price !== null && row.price !== undefined ? Number(row.price) : null,
    order_count: Number(row.order_count || 0),
  };
}
