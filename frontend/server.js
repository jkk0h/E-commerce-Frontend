import express from "express";
import path from "path";
import { fileURLToPath } from "url";

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(__dirname)); // serves index.html and assets from /frontend

app.get("/health", (_req, res) => res.json({ ok: true }));

app.listen(process.env.PORT || 5173, "0.0.0.0", () =>
  console.log("Frontend running")
);
