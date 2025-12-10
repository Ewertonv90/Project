# src/main.py
"""
main.py
Orquestrador do pipeline completo de ETL + Analytics

Fluxo:
1. Extração das APIs
2. Transformação e normalização
3. Carga no SQLite
4. Criação da VIEW analítica

Execução:
>>> python src/main.py
"""

from __future__ import annotations
import os
import logging
import sqlite3

from extract import extract_all
from transform import transform_all
from load import load_all
from pathlib import Path

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("main")

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = os.path.join(BASE_DIR, "data/trusted", "fakestore.db")
ANALYTICS_SQL_PATH = BASE_DIR / "sql" / "analytics.sql"

def execute_analytics_sql():
    logger.info("Criando VIEW analítica...")
 
    conn = sqlite3.connect(DB_PATH)
    try:
        with open(ANALYTICS_SQL_PATH, "r", encoding="utf-8") as f:
            analytics_sql = f.read()
        conn.executescript(analytics_sql)
        conn.commit()
        logger.info("VIEW analítica criada com sucesso.")
    finally:
        conn.close()


def main():
    logger.info("========== INÍCIO DO PIPELINE ==========")

    # 1. EXTRAÇÃO
    logger.info("Etapa 1 - Extração")
    dfs_raw = extract_all(save_raw=True)

    # 2. TRANSFORMAÇÃO
    logger.info("Etapa 2 - Transformação")
    users_df, products_df, carts_df, cart_items_df = transform_all(dfs_raw)

    # 3. CARGA
    logger.info("Etapa 3 - Carga no Banco")
    load_all(users_df, products_df, carts_df, cart_items_df)

    # 4. AGREGAÇÃO ANALÍTICA
    logger.info("Etapa 4 - Criação da VIEW Analítica")
    execute_analytics_sql()

    logger.info("========== PIPELINE FINALIZADO COM SUCESSO ==========")


if __name__ == "__main__":
    main()
