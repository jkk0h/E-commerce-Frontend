import express from "express";
import cors from "cors";
import health from "./routes/health.routes.js";
import products from "./routes/products.routes.js";
import orders from "./routes/orders.routes.js";
import sql from "./routes/sql.routes.js";
import { errorHandler } from "./middleware/error.js";

export function createServer() {
  const app = express();
  app.use(cors());
  app.use(express.json());

  app.use("/api/health", health);
  app.use("/api/products", products);
  app.use("/api/orders", orders);
  app.use("/api/postgres", sql); // disable by ALLOW_SQL when needed

  app.use(errorHandler);
  return app;
}
