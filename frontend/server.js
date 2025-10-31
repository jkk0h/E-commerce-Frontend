import express from "express";
const app = express();

// serve everything in this folder (index.html, assets, etc.)
app.use(express.static("./"));

app.get("/health", (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 5173;
app.listen(port, "0.0.0.0", () => console.log("Frontend running on", port));
