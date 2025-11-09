export function errorHandler(err, req, res, _next) {
  console.error("ERROR:", err);
  const status = err.statusCode || 500;
  res.status(status).json({
    error: err.message || "Unexpected error",
  });
}
