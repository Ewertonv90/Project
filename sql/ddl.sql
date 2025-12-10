PRAGMA foreign_keys = ON;

-- =========================
-- USERS
-- =========================
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    username TEXT NOT NULL,
    city TEXT,
    street TEXT,
    zipcode TEXT,
    lat REAL,
    long REAL
);

-- =========================
-- PRODUCTS
-- =========================
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    price REAL NOT NULL,
    category TEXT
);

-- =========================
-- CARTS
-- =========================
CREATE TABLE IF NOT EXISTS carts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- =========================
-- CART_ITEMS
-- =========================
CREATE TABLE IF NOT EXISTS cart_items (
    cart_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    PRIMARY KEY (cart_id, product_id),
    FOREIGN KEY (cart_id) REFERENCES carts(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- =========================
-- ÍNDICES (PERFORMANCE)
-- =========================
CREATE INDEX IF NOT EXISTS idx_carts_user_id
ON carts(user_id);

CREATE INDEX IF NOT EXISTS idx_cart_items_cart_id
ON cart_items(cart_id);

CREATE INDEX IF NOT EXISTS idx_cart_items_product_id
ON cart_items(product_id);
