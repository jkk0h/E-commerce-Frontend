import express from "express";
import { fileURLToPath } from "url";
import path from "path";

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = process.env.PORT || 5500;  
const HOST = "127.0.0.1"; 

app.use(express.static(__dirname));        // serve index.html + assets
app.get("/health", (_req, res) => res.json({ ok: true }));

app.listen(PORT, HOST, () => {
    console.log(`Frontend running at http://${HOST}:${PORT}`);
});
