// script/config.js
// Simple helpers to talk to the two API servers directly.
// No more engine switching / localStorage mode.

(function () {
    // Adjust if your ports/hosts differ:
    const SQL_API_ROOT = "http://localhost:3001";   // Postgres API server
    const MONGO_API_ROOT = "http://localhost:3002"; // MongoDB API server

    function buildUrl(base, path = "") {
        if (!path) return base;
        const normalized = path.startsWith("/") ? path.slice(1) : path;
        const withSlash = base.endsWith("/") ? base : base + "/";
        return withSlash + normalized;
    }

    // Always talk to SQL backend (port 3001)
    window.sqlApi = function (path = "") {
        return buildUrl(SQL_API_ROOT, path);
    };

    // Always talk to Mongo backend (port 3002)
    window.mongoApi = function (path = "") {
        return buildUrl(MONGO_API_ROOT, path);
    };
})();
