📚 Project Setup and Execution Guide
This document provides instructions for setting up and running the SQL/Mongo Demo Backend API (api) and its associated services.

🏁 Prerequisites Checklist
Before proceeding, ensure you have the following installed and configured:
[ ] Node.js (v18.0.0 or higher): Required to run the API and scripts.
[ ] PostgreSQL Database:
Running Service: PostgreSQL server must be active.
Management: PgAdmin4 is recommended for administration.
Database: Create a database named shop (or your preferred name).
[ ] MongoDB Database:
Running Service: MongoDB server (version 6+) must be active.
Management: MongoDB Compass is recommended for administration.
[ ] Project Dependencies: Navigate to the project root directory in your terminal and install packages:
Bash
npm install

💻 Step 1: Database Setup and Seeding (PowerShell 1)
This step initializes your PostgreSQL and MongoDB databases with the required demo data. 
You will need to open the project's root directory in your first PowerShell window.
1. Set Environment VariablesYou must set the connection details for both databases. Remember to substitute your actual PostgreSQL password and database name.
SQLPOSTGRES_URL: $env:POSTGRES_URL = "postgres://postgres:4kfBQKi6B64r%40!A@localhost:5432/shop"
DBMONGODB_URI: $env:MONGODB_URI="mongodb://localhost:27017"
NameMONGO_DB_NAME: $env:MONGO_DB_NAME="appdb"
⚠️ Note on PostgreSQL URL: The format is postgres://<user>:<password>@<host>:<port>/<db_name>.
If your password contains special characters like @ or !, you may need to URL encode them (e.g., @ becomes %40, ! becomes %21). Your example already includes encoding.

2. Run Seeding ScriptsExecute the seed scripts using the commands defined in your package.json:
Bash
npm run seed:pg
npm run seed:mongo


🚀 Step 2: Start Backend APIs (PowerShell 2 & 3)
You need to open two new PowerShell windows, one for the SQL API and one for the NoSQL API.
⚠️ IMPORTANT: In both PowerShell windows, you must first re-set the environment variables from Step 1 before running the API servers.
1. Start SQL API Server (PowerShell 2)
Open a Second PowerShell window, set the environment variables, and run the SQL API:
Bash
$env:POSTGRES_URL = "postgres://postgres:4kfBQKi6B64r%40!A@localhost:5432/shop"
$env:MONGODB_URI="mongodb://localhost:27017"
$env:MONGO_DB_NAME="appdb"
node server.js

2. Start NoSQL API Server (PowerShell 3)
Open a Third PowerShell window, set the environment variables, and run the MongoDB API:
Bash
$env:POSTGRES_URL = "postgres://postgres:4kfBQKi6B64r%40!A@localhost:5432/shop"
$env:MONGODB_URI="mongodb://localhost:27017"
$env:MONGO_DB_NAME="appdb"
npm run mongo

🌐 Step 3: Start the Frontend (PowerShell 4)
Open a Fourth PowerShell window, navigate to the frontend directory (if separate), and start the client application.
Bash
npm start





























