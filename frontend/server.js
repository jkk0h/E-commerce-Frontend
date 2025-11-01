// --- /frontend/server.js ---
import express from "express";
import { fileURLToPath } from 'url';
import path from 'path';

// Assuming you are using mongoose for MongoDB
// You must 'npm install mongoose' in the frontend/package.json directory
import mongoose from "mongoose"; 

// Assuming you are using 'pg' for PostgreSQL
// You must 'npm install pg' in the frontend/package.json directory
import pg from 'pg'; 

const app = express();
const port = process.env.PORT || 5173;

// --- A. DATABASE CONNECTION SETUP ---

// MongoDB Connection
const MONGO_URI = process.env.MONGO_URI; 
if (MONGO_URI) {
    mongoose.connect(MONGO_URI)
        .then(() => console.log("MongoDB connected successfully."))
        .catch(err => console.error("MongoDB connection error:", err));
} else {
    console.warn("MONGO_URI not found. MongoDB features will be disabled.");
}

// PostgreSQL Connection
const PG_URI = process.env.PG_URI;
const pgPool = new pg.Pool({ connectionString: PG_URI });

pgPool.connect()
    .then(client => {
        console.log("PostgreSQL connected successfully.");
        client.release();
    })
    .catch(err => console.error("PostgreSQL connection error:", err));


// --- B. API ROUTES ---

// Your actual database fetching logic from your API folder needs to be integrated here.
app.get("/api/products", async (req, res) => {
    try {
        // --- Example: MongoDB Fetch (assuming you have a Mongoose Product model) ---
        // const products = await Product.find({}); 
        
        // --- Example: PostgreSQL Fetch ---
        const pgRes = await pgPool.query('SELECT product_id, product_category_name FROM olist_products_dataset LIMIT 10;');
        const products = pgRes.rows;

        // In a real app, you would fetch from ONE database, not both.
        // For demonstration, we'll use the PostgreSQL results.
        
        res.json(products);
        
    } catch (error) {
        console.error("Error fetching products:", error);
        res.status(500).json({ error: "Failed to retrieve products from database." });
    }
});

// Health check route (from your original file)
app.get("/health", (_req, res) => res.json({ ok: true }));


// --- C. STATIC FILE SERVING ---

// Serve static files (ensure this is AFTER your API routes)
// Note: You must ensure all frontend static files are accessible relative to this running server.
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// This assumes your static files (index.html, assets, etc.) are in the root or a 'public' folder 
// relative to where this server.js file is running.
app.use(express.static(path.join(__dirname, '../frontend'))); // Adjust path as necessary to point to your static assets


// --- D. START SERVER ---
app.listen(port, "0.0.0.0", () => {
    console.log("Frontend/API server running on", port);
});
