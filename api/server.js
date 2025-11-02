import express from "express";
import cors from "cors";
import { connectAll } from "./db.js";

const app = express();
app.use(express.json());
app.use(cors({ origin: [/\.up\.railway\.app$/] }));

const { pgPool, mongoDb } = await connectAll();

// Helper function to execute raw SQL (PostgreSQL) queries
async function executeSqlQuery(query) {
    if (!pgPool) {
        throw new Error("PostgreSQL connection not available.");
    }
    const result = await pgPool.query(query);
    return result.rows; // For SELECT statements
}

// Helper function to execute raw MongoDB commands
async function executeMongoCommand(commandString) {
    if (!mongoDb) {
        throw new Error("MongoDB connection not available.");
    }

    // WARNING: Using eval or raw string parsing for user commands is inherently insecure.
    // For a simple demonstration, we assume commands are simple method calls (e.g., "find({})").
    // A production system must use a safer command execution layer.
    
    // Parse the command: e.g., "products.find({})"
    const parts = commandString.split('.');
    if (parts.length < 2) {
        throw new Error("Invalid MongoDB command format. Expected 'collection.method(args)'.");
    }

    const collectionName = parts[0];
    const restOfString = parts.slice(1).join('.');
    
    // Attempt to extract method name and arguments
    const match = restOfString.match(/(\w+)\((.*)\)/s);
    if (!match) {
        throw new Error("Invalid MongoDB command structure. Must be 'method(args)'.");
    }

    const methodName = match[1];
    let argsString = match[2].trim();
    let args = [];

    // Simple JSON parsing for arguments (e.g., from 'find({})' or 'insertOne({name:"x"})')
    if (argsString) {
        try {
            // Attempt to parse the arguments string as a JSON array or object
            // Wrap in brackets if it's not already an array for easier parsing
            const jsonString = argsString.startsWith('[') ? argsString : `[${argsString}]`;
            args = JSON.parse(jsonString);
        } catch (e) {
            // If JSON parsing fails, treat it as a raw string argument
            args = [argsString];
        }
    }

    const collection = mongoDb.collection(collectionName);
    
    if (typeof collection[methodName] === 'function') {
        const result = await collection[methodName](...args);
        
        // Handle common methods that return a Cursor (like find)
        if (methodName === 'find' || methodName === 'aggregate') {
            return result.toArray();
        }
        
        return result; // For insert, update, etc.
    } else {
        throw new Error(`Method "${methodName}" not found on collection.`);
    }
}

// ----------------------------------------------------------------------
// 🚨 NEW ROUTES FOR COMMANDS.HTML 🚨
// ----------------------------------------------------------------------

// 1. RAW SQL/POSTGRESQL QUERY ROUTE
// This route is aliased as /api/mysql/query to match your frontend's current call, 
// but it executes PostgreSQL against pgPool.
app.post("/api/mysql/query", async (req, res) => {
    const { query } = req.body;
    try {
        const results = await executeSqlQuery(query);
        res.json({ success: true, results });
    } catch (e) {
        // Send a 400 Bad Request for user errors (like bad SQL syntax)
        console.error("PostgreSQL Query Error:", e.message);
        res.status(400).json({ success: false, error: `SQL Error: ${e.message}` });
    }
});

// 2. RAW MONGODB COMMAND ROUTE
app.post("/api/mongodb/command", async (req, res) => {
    const { command } = req.body;
    try {
        const results = await executeMongoCommand(command);
        res.json({ success: true, results });
    } catch (e) {
        // Send a 400 Bad Request for user errors (like bad command syntax)
        console.error("MongoDB Command Error:", e.message);
        res.status(400).json({ success: false, error: `MongoDB Error: ${e.message}` });
    }
});


app.get("/health", (_req, res) => res.json({ ok: true }));

app.get("/diagnostics", async (_req, res) => {
  try {
    const { rows } = await pgPool.query("select 1 as ok");
    let mongo = "skipped", productsCount = -1;
    if (mongoDb) {
      const ping = await mongoDb.command({ ping: 1 });
      mongo = ping?.ok === 1 ? "ok" : "fail";
      productsCount = await mongoDb.collection("products").countDocuments().catch(() => -1);
    }
    res.json({ api: "ok", postgres: rows[0]?.ok === 1 ? "ok" : "fail", mongo, productsCount });
  } catch (e) {
    res.status(500).json({ api: "fail", error: e.message });
  }
});

app.get("/api/products", async (_req, res) => {
    try {
        if (!mongoDb) {
            // Return 503 Service Unavailable if the database connection failed
            return res.status(503).json({ error: "MongoDB connection not available." });
        }

        // Fetch all documents from the 'products' collection
        const products = await mongoDb.collection("products").find({}).toArray();

        // Respond with the products array (even if it's empty: [])
        res.json(products);
    } catch (e) {
        console.error("Error fetching products:", e);
        res.status(500).json({ error: "Failed to retrieve products from database." });
    }
});

app.get("/api/products/:id", async (req, res) => {
    try {
        const productId = req.params.id;
        if (!mongoDb) {
            return res.status(503).json({ error: "MongoDB connection not available." });
        }

        // Fetch the product using the ID. Adjust 'product_id' if your collection uses a different key.
        const product = await mongoDb.collection("products").findOne({ product_id: productId });
        
        if (!product) {
            return res.status(404).json({ error: "Product not found" });
        }

        res.json(product);
    } catch (e) {
        console.error("Error fetching single product:", e);
        res.status(500).json({ error: "Failed to retrieve product details." });
    }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log("API listening on", port));
