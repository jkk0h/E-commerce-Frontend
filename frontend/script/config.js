window.API_URL = "http://localhost:3001/";
window.getApiUrl = (path = "") =>
    (window.API_URL.endsWith("/") ? window.API_URL : window.API_URL + "/") +
    (path.startsWith("/") ? path.slice(1) : path);