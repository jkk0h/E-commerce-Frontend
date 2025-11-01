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

// Helper function to map PostgreSQL data to the frontend's expected format
const mapPgProductToFrontend = (pgProduct, index) => {
    // Note: The PG table only has category and ID, so we map them and mock a price.
    const basePrice = 19.99;
    const price = (basePrice + index * 5.0).toFixed(2); // Mock price calculation
    
    // Capitalize the first letter of the category name for a cleaner title
    const categoryName = pgProduct.product_category_name 
        ? pgProduct.product_category_name.charAt(0).toUpperCase() + pgProduct.product_category_name.slice(1).replace(/_/g, ' ')
        : 'Unknown Product';

    return {
        title: categoryName,
        sku: pgProduct.product_id,
        price: parseFloat(price)
    };
};


app.get("/api/products", async (req, res) => {
    try {
        // Fetch data from PostgreSQL (using the defined keys from the Olist dataset)
        const pgRes = await pgPool.query('SELECT product_id, product_category_name FROM olist_products_dataset LIMIT 10;');
        
        // Map the raw PG data to the format the frontend expects (title, sku, price)
        const products = pgRes.rows.map(mapPgProductToFrontend);

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
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// This assumes your static files (index.html, assets, etc.) are in the root or a 'public' folder 
// relative to where this server.js file is running.
app.use(express.static(path.join(__dirname, '../frontend'))); // Adjust path as necessary to point to your static assets


// --- D. START SERVER ---
app.listen(port, "0.0.0.0", () => {
    console.log("Frontend/API server running on", port);
});
