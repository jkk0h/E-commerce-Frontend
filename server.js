import express from "express";
const app = express();
app.use(express.static("./"));

app.get("/health", (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 5173;
app.listen(port, "0.0.0.0", () => console.log("Frontend on", port));

