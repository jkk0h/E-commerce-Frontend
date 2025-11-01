import express from "express";
import { fileURLToPath } from "url";
import path from "path";

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(__dirname));        // serve index.html + assets
app.get("/health", (_req, res) => res.json({ ok: true }));

app.listen(process.env.PORT || 5173, "0.0.0.0", () => console.log("Frontend running"));
