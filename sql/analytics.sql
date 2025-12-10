-- sql/analytics.sql

DROP VIEW IF EXISTS user_analytics;

CREATE VIEW user_analytics AS
WITH base AS (
    SELECT
        u.id               AS user_id,
        c.id               AS cart_id,
        p.id               AS product_id,
        p.price            AS product_price,
        p.category         AS product_category
    FROM users u
    LEFT JOIN carts c
        ON u.id = c.user_id
    LEFT JOIN cart_items ci
        ON c.id = ci.cart_id
    LEFT JOIN products p
        ON ci.product_id = p.id
),

cart_agg AS (
    SELECT
        user_id,
        COUNT(DISTINCT cart_id)                        AS total_carts,
        COUNT(DISTINCT product_id)                     AS total_products,
        CAST(COUNT(product_id) AS REAL)
            / NULLIF(COUNT(DISTINCT cart_id), 0)      AS avg_products_per_cart,
        COUNT(DISTINCT product_category)               AS distinct_categories,
        MAX(product_price)                             AS most_expensive_product,
        SUM(product_price)                             AS total_value
    FROM base
    GROUP BY user_id
)

SELECT
    user_id,
    COALESCE(total_carts, 0)              AS total_carts,
    COALESCE(total_products, 0)           AS total_products,
    ROUND(COALESCE(avg_products_per_cart, 0), 2) AS avg_products_per_cart,
    COALESCE(distinct_categories, 0)      AS distinct_categories,
    COALESCE(most_expensive_product, 0)   AS most_expensive_product,
    ROUND(COALESCE(total_value, 0), 2)    AS total_value
FROM cart_agg;
