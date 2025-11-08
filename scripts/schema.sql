-- =====================================================================
-- 0) Safety & session
-- =====================================================================
SET client_min_messages TO WARNING;
SET standard_conforming_strings = on;

-- =====================================================================
-- 1) DROP (reverse dependency order, or just CASCADE to be sure)
-- =====================================================================
DROP TABLE IF EXISTS order_review_text CASCADE;
DROP TABLE IF EXISTS order_reviews_core CASCADE;
DROP TABLE IF EXISTS order_payment_amount CASCADE;
DROP TABLE IF EXISTS order_payment_method CASCADE;
DROP TABLE IF EXISTS order_item_pricing CASCADE;
DROP TABLE IF EXISTS order_item_shipping CASCADE;
DROP TABLE IF EXISTS order_items_core CASCADE;
DROP TABLE IF EXISTS orders_timestamps CASCADE;
DROP TABLE IF EXISTS orders_header CASCADE;
DROP TABLE IF EXISTS payment_type_dim CASCADE;

-- Staging / legacy flat table (kept for reference or re-loads)
DROP TABLE IF EXISTS orders CASCADE;

-- =====================================================================
-- 2) CREATE TABLES (no foreign keys yet)
--    Creation order (parents before children)
-- =====================================================================

-- 2.0  (Optional) Staging "big flat" orders from orders.csv
--      Leave constraints off; we just stage raw rows here.
CREATE TABLE orders (
    order_id                         CHAR(32),
    order_item_id                    INTEGER,
    product_id                       CHAR(32),
    seller_id                        CHAR(32),
    shipping_limit_date              TEXT,              -- non-ISO in CSV; keep as text in staging
    price                            NUMERIC(10,2),
    freight_value                    NUMERIC(10,2),
    payment_sequential               INTEGER,
    payment_type                     VARCHAR(32),
    payment_installments             INTEGER,
    payment_value                    NUMERIC(10,2),
    review_id                        CHAR(32),
    review_score                     INTEGER,
    review_comment_title             TEXT,
    review_comment_message           TEXT,
    review_creation_date             TIMESTAMP,
    review_answer_timestamp          TIMESTAMP,
    customer_id                      CHAR(32),
    order_status                     VARCHAR(32),
    order_purchase_timestamp         TIMESTAMP,
    order_approved_at                TIMESTAMP,
    order_delivered_carrier_date     TIMESTAMP,
    order_delivered_customer_date    TIMESTAMP,
    order_estimated_delivery_date    TIMESTAMP
);

-- 2.1  Dimensions & top-level parents
CREATE TABLE payment_type_dim (
    payment_type VARCHAR(32) PRIMARY KEY
);

CREATE TABLE orders_header (
    order_id     CHAR(32) PRIMARY KEY,
    customer_id  CHAR(32) NOT NULL,
    order_status VARCHAR(32) NOT NULL
);

-- 2.2  Order-level child (1:1 with orders_header)
CREATE TABLE orders_timestamps (
    order_id                         CHAR(32) PRIMARY KEY,
    order_purchase_timestamp         TIMESTAMP,
    order_approved_at                TIMESTAMP,
    order_delivered_carrier_date     TIMESTAMP,
    order_delivered_customer_date    TIMESTAMP,
    order_estimated_delivery_date    TIMESTAMP
);

-- 2.3  Item-level core + its children
CREATE TABLE order_items_core (
    order_id      CHAR(32)  NOT NULL,
    order_item_id INTEGER   NOT NULL,
    product_id    CHAR(32)  NOT NULL,
    seller_id     CHAR(32)  NOT NULL,
    CONSTRAINT pk_order_items_core PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_item_shipping (
    order_id             CHAR(32)  NOT NULL,
    order_item_id        INTEGER   NOT NULL,
    shipping_limit_date  TIMESTAMP,
    CONSTRAINT pk_order_item_shipping PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_item_pricing (
    order_id        CHAR(32)      NOT NULL,
    order_item_id   INTEGER       NOT NULL,
    price           NUMERIC(10,2) NOT NULL,
    freight_value   NUMERIC(10,2) NOT NULL,
    CONSTRAINT pk_order_item_pricing PRIMARY KEY (order_id, order_item_id)
);

-- 2.4  Payments (2-level hierarchy)
CREATE TABLE order_payment_method (
    order_id              CHAR(32)     NOT NULL,
    payment_sequential    INTEGER      NOT NULL,
    payment_type          VARCHAR(32)  NOT NULL,
    payment_installments  INTEGER      NOT NULL,
    CONSTRAINT pk_order_payment_method PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE order_payment_amount (
    order_id            CHAR(32)      NOT NULL,
    payment_sequential  INTEGER       NOT NULL,
    payment_value       NUMERIC(10,2) NOT NULL,
    CONSTRAINT pk_order_payment_amount PRIMARY KEY (order_id, payment_sequential)
);

-- 2.5  Reviews (composite key)
CREATE TABLE order_reviews_core (
    review_id                 CHAR(32)     NOT NULL,
    order_id                  CHAR(32)     NOT NULL,
    review_score              INTEGER,
    review_creation_date      TIMESTAMP,
    review_answer_timestamp   TIMESTAMP,
    CONSTRAINT pk_order_reviews_core PRIMARY KEY (review_id, order_id)
);

CREATE TABLE order_review_text (
    review_id                CHAR(32) NOT NULL,
    order_id                 CHAR(32) NOT NULL,
    review_comment_title     TEXT,
    review_comment_message   TEXT,
    CONSTRAINT pk_order_review_text PRIMARY KEY (review_id, order_id)
);

-- =====================================================================
-- 3) LOAD DATA (adjust path if needed)
--    If the server cannot access /tmp/postgres_import/,
--    use psql `\copy` from your client machine instead.
-- =====================================================================

-- 3.0 staging: orders (flat)
-- Comma CSV with header; empty strings -> NULL
COPY orders
FROM '/tmp/postgres_import/orders.csv'
WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- 3.1 payment_type_dim (you may also derive distinct values from orders if you prefer)
-- If your file is named payment_type.csv, rename it OR change the path below.
-- COPY payment_type_dim (payment_type)
-- FROM '/tmp/postgres_import/payment_type_dim.csv'
-- WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- 3.2 orders_header
COPY orders_header (order_id, customer_id, order_status)
FROM '/tmp/postgres_import/orders_header.csv'
WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- 3.3 orders_timestamps
COPY orders_timestamps (order_id, order_purchase_timestamp, order_approved_at,
                        order_delivered_carrier_date, order_delivered_customer_date,
                        order_estimated_delivery_date)
FROM '/tmp/postgres_import/orders_timestamps.csv'
WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- 3.4 order_items_core
COPY order_items_core (order_id, order_item_id, product_id, seller_id)
FROM '/tmp/postgres_import/order_item_core.csv'
WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- 3.5 order_item_shipping
COPY order_item_shipping (order_id, order_item_id, shipping_limit_date)
FROM '/tmp/postgres_import/order_item_shipping.csv'
WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- 3.6 order_item_pricing
COPY order_item_pricing (order_id, order_item_id, price, freight_value)
FROM '/tmp/postgres_import/order_item_pricing.csv'
WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- 3.7 order_payment_method
COPY order_payment_method (order_id, payment_sequential, payment_type, payment_installments)
FROM '/tmp/postgres_import/order_payment_method.csv'
WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- 3.8 order_payment_amount
COPY order_payment_amount (order_id, payment_sequential, payment_value)
FROM '/tmp/postgres_import/order_payment_amount.csv'
WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- 3.9 order_reviews_core  (semicolon-delimited file)
COPY order_reviews_core (review_id, order_id, review_score,
                         review_creation_date, review_answer_timestamp)
FROM '/tmp/postgres_import/order_review_core.csv'
WITH (FORMAT csv, DELIMITER ';', HEADER true, NULL '', QUOTE '"');

-- 3.10 order_review_text (comma CSV)
COPY order_review_text (review_id, order_id, review_comment_title, review_comment_message)
FROM '/tmp/postgres_import/order_review_text.csv'
WITH (FORMAT csv, HEADER true, NULL '', QUOTE '"');

-- =====================================================================
-- 4) ADD FOREIGN KEYS (after successful loads)
--    Also add useful indexes for join performance.
-- =====================================================================

-- 4.1 orders_timestamps → orders_header
ALTER TABLE orders_timestamps
  ADD CONSTRAINT fk_orders_timestamps_order
  FOREIGN KEY (order_id) REFERENCES orders_header(order_id);

CREATE INDEX idx_orders_timestamps_order_id
  ON orders_timestamps(order_id);

-- 4.2 order_items_core → orders_header
ALTER TABLE order_items_core
  ADD CONSTRAINT fk_order_items_core_order
  FOREIGN KEY (order_id) REFERENCES orders_header(order_id);

CREATE INDEX idx_order_items_core_order_id
  ON order_items_core(order_id);

-- 4.3 order_item_shipping → order_items_core
ALTER TABLE order_item_shipping
  ADD CONSTRAINT fk_order_item_shipping_item
  FOREIGN KEY (order_id, order_item_id)
  REFERENCES order_items_core(order_id, order_item_id);

CREATE INDEX idx_order_item_shipping_item
  ON order_item_shipping(order_id, order_item_id);

-- 4.4 order_item_pricing → order_items_core
ALTER TABLE order_item_pricing
  ADD CONSTRAINT fk_order_item_pricing_item
  FOREIGN KEY (order_id, order_item_id)
  REFERENCES order_items_core(order_id, order_item_id);

CREATE INDEX idx_order_item_pricing_item
  ON order_item_pricing(order_id, order_item_id);

-- 4.5 order_payment_method → orders_header, payment_type_dim
--     (If you didn’t load payment_type_dim, either load it or drop this FK.)
ALTER TABLE order_payment_method
  ADD CONSTRAINT fk_order_payment_method_order
  FOREIGN KEY (order_id) REFERENCES orders_header(order_id);

ALTER TABLE order_payment_method
  ADD CONSTRAINT fk_order_payment_method_type
  FOREIGN KEY (payment_type) REFERENCES payment_type_dim(payment_type);

CREATE INDEX idx_order_payment_method_order
  ON order_payment_method(order_id);

-- 4.6 order_payment_amount → order_payment_method
ALTER TABLE order_payment_amount
  ADD CONSTRAINT fk_order_payment_amount_method
  FOREIGN KEY (order_id, payment_sequential)
  REFERENCES order_payment_method(order_id, payment_sequential);

CREATE INDEX idx_order_payment_amount_method
  ON order_payment_amount(order_id, payment_sequential);

-- 4.7 order_reviews_core → orders_header
ALTER TABLE order_reviews_core
  ADD CONSTRAINT fk_order_reviews_core_order
  FOREIGN KEY (order_id) REFERENCES orders_header(order_id);

CREATE INDEX idx_order_reviews_core_order
  ON order_reviews_core(order_id);

-- 4.8 order_review_text → order_reviews_core
ALTER TABLE order_review_text
  ADD CONSTRAINT fk_order_review_text_core
  FOREIGN KEY (review_id, order_id)
  REFERENCES order_reviews_core(review_id, order_id);

CREATE INDEX idx_order_review_text_core
  ON order_review_text(review_id, order_id);

-- =====================================================================
-- 5) (Optional) Extras
-- =====================================================================

-- Example of PostgreSQL auto-increment:
--   Use SERIAL (or IDENTITY) instead of MySQL AUTO_INCREMENT:
-- CREATE TABLE example_with_identity (
--   audit_id  SERIAL PRIMARY KEY,
--   created_at TIMESTAMP DEFAULT now()
-- );

-- Helpful: ensure stats are up to date after big loads
ANALYZE;

-- Done!
