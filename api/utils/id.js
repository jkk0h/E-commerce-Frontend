import crypto from "crypto";

export function newOrderId() {
  // short, URL-safe-ish id
  return crypto.randomBytes(8).toString("hex");
}
