# src/transform.py
"""
transform.py
Responsável por:
- Normalizar users, products e carts
- Criar a tabela cart_items (explode dos produtos do carrinho)
- Tratar nulos e padronizar tipos
- Retornar 4 DataFrames prontos para carga:
    users_df, products_df, carts_df, cart_items_df

Uso:
>>> from extract import extract_all
>>> from transform import transform_all
>>> dfs_raw = extract_all()
>>> users_df, products_df, carts_df, cart_items_df = transform_all(dfs_raw)
"""

from __future__ import annotations
import logging
import pandas as pd
from typing import Dict, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("transform")


# =========================
# USERS
# =========================
def transform_users(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando users...")

    users = pd.DataFrame({
        "id": df["id"],
        "first_name": df["name.firstname"],
        "last_name": df["name.lastname"],
        "email": df["email"],
        "username": df["username"],
        "city": df["address.city"],
        "street": df["address.street"],
        "zipcode": df["address.zipcode"],
        "lat": df["address.geolocation.lat"],
        "long": df["address.geolocation.long"],
    })

    users = users.fillna({
        "first_name": "UNKNOWN",
        "last_name": "UNKNOWN",
        "email": "UNKNOWN",
        "city": "UNKNOWN",
        "street": "UNKNOWN",
        "zipcode": "UNKNOWN",
        "lat": 0.0,
        "long": 0.0,
    })

    # Tipagem
    users["id"] = users["id"].astype(int)
    users["lat"] = users["lat"].astype(float)
    users["long"] = users["long"].astype(float)

    logger.info("Users normalizado: %d linhas", len(users))
    return users


# =========================
# PRODUCTS
# =========================
def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando products...")

    products = df[[
        "id", "title", "description", "price", "category"
    ]].copy()

    products = products.fillna({
        "title": "UNKNOWN",
        "description": "UNKNOWN",
        "price": 0.0,
        "category": "UNKNOWN",
    })

    products["id"] = products["id"].astype(int)
    products["price"] = products["price"].astype(float)

    logger.info("Products normalizado: %d linhas", len(products))
    return products


# =========================
# CARTS
# =========================
def transform_carts(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando carts...")

    carts = df[["id", "userId"]].copy()
    carts = carts.rename(columns={"userId": "user_id"})

    carts = carts.fillna({
        "user_id": 0
    })

    carts["id"] = carts["id"].astype(int)
    carts["user_id"] = carts["user_id"].astype(int)

    logger.info("Carts normalizado: %d linhas", len(carts))
    return carts


# =========================
# CART ITEMS (EXPLODE)
# =========================
def transform_cart_items(df: pd.DataFrame) -> pd.DataFrame:

    logger.info("Transformando cart_items (explode)...")

    exploded = df[["id", "products"]].explode("products")

    exploded["product_id"] = exploded["products"].apply(lambda x: x["productId"])
    exploded["cart_id"] = exploded["id"]

    cart_items = exploded[["cart_id", "product_id"]].copy()

    cart_items["cart_id"] = cart_items["cart_id"].astype(int)
    cart_items["product_id"] = cart_items["product_id"].astype(int)

    logger.info("Cart_items gerado: %d linhas", len(cart_items))
    return cart_items

# =========================
# PIPELINE DE TRANSFORMAÇÃO
# =========================
def transform_all(dfs_raw: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Recebe:
      dfs_raw["users"]
      dfs_raw["products"]
      dfs_raw["carts"]

    Retorna:
      users_df, products_df, carts_df, cart_items_df
    """
    logger.info("Iniciando pipeline de transformação...")

    users_df = transform_users(dfs_raw["users"])
    products_df = transform_products(dfs_raw["products"])
    carts_df = transform_carts(dfs_raw["carts"])
    cart_items_df = transform_cart_items(dfs_raw["carts"])

    logger.info("Transformação concluída com sucesso.")
    return users_df, products_df, carts_df, cart_items_df
