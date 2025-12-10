# src/load.py
"""
load.py

Uso:
>>> from load import load_all
>>> load_all(users_df, products_df, carts_df, cart_items_df)
"""

from __future__ import annotations
import os
import sqlite3
import logging
import pandas as pd
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("load")

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "fakestore.db")
DDL_PATH = os.path.join(BASE_DIR, "sql", "ddl.sql")


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def execute_ddl(conn: sqlite3.Connection):
    logger.info("Executando DDL...")
    with open(DDL_PATH, "r", encoding="utf-8") as f:
        ddl_sql = f.read()
    conn.executescript(ddl_sql)
    conn.commit()
    logger.info("DDL executado com sucesso.")


def insert_dataframe(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    table_name: str
):
    logger.info("Carregando tabela: %s (%d registros)", table_name, len(df))

    placeholders = ", ".join(["?"] * len(df.columns))
    columns = ", ".join(df.columns.tolist())
    sql = f"""
        INSERT OR REPLACE INTO {table_name}
        ({columns})
        VALUES ({placeholders})
    """

    data = df.values.tolist()

    conn.executemany(sql, data)
    conn.commit()

    logger.info("Carga concluída para %s", table_name)


def load_all(
    users_df: pd.DataFrame,
    products_df: pd.DataFrame,
    carts_df: pd.DataFrame,
    cart_items_df: pd.DataFrame
):
 
    logger.info("Iniciando carga no banco SQLite...")

    conn = get_connection()

    try:
        execute_ddl(conn)

        insert_dataframe(conn, users_df, "users")
        insert_dataframe(conn, products_df, "products")
        insert_dataframe(conn, carts_df, "carts")
        insert_dataframe(conn, cart_items_df, "cart_items")

    finally:
        conn.close()
        logger.info("Conexão com o banco encerrada.")

    logger.info("Carga no banco finalizada com sucesso.")
