window.API_URL = "https://e-commerce-frontend-production-f9c0.up.railway.app/";
window.getApiUrl = (path = "") =>
    (window.API_URL.endsWith("/") ? window.API_URL : window.API_URL + "/") +
    (path.startsWith("/") ? path.slice(1) : path);