# src/extract.py
"""
extract.py

Uso:
>>> from extract import extract_all
>>> dfs = extract_all(save_raw=True)
  
Requisitos:
pip install requests pandas pyarrow
"""
from __future__ import annotations
import os
import json
import logging
from typing import Dict
import time

import requests
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("extract")


BASE = "https://fakestoreapi.com"
ENDPOINTS = {
    "users": f"{BASE}/users",
    "products": f"{BASE}/products",
    "carts": f"{BASE}/carts",
}

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")


def _ensure_raw_dir():
    os.makedirs(RAW_DIR, exist_ok=True)


def _get_json_with_retries(url: str, retries: int = 3, backoff: float = 1.0, timeout: int = 10):
    """Faz GET com retries simples e devolve JSON (ou lança)."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            logger.debug("GET %s (attempt %d)", url, attempt)
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_exc = e
            logger.warning("Falha ao buscar %s (attempt %d/%d): %s", url, attempt, retries, e)
            time.sleep(backoff * attempt)
    logger.error("Não foi possível obter %s após %d tentativas", url, retries)
    raise last_exc


def _save_raw(name: str, payload):
    """Salva payload JSON em data/raw/{name}.json e um CSV/parquet simples."""
    _ensure_raw_dir()
    json_path = os.path.join(RAW_DIR, f"{name}.json")
    csv_path = os.path.join(RAW_DIR, f"{name}.csv")
    parquet_path = os.path.join(RAW_DIR, f"{name}.parquet")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


    try:
        df = pd.json_normalize(payload)
        df.to_csv(csv_path, index=False)
        try:
            df.to_parquet(parquet_path, index=False)
        except Exception:
            logger.debug("pyarrow/parquet não disponível; pulando parquet.")
    except Exception as e:
        logger.warning("Não foi possível normalizar/salvar CSV/parquet para %s: %s", name, e)

    logger.info("Salvou raw: %s", json_path)


def extract_all(save_raw: bool = True) -> Dict[str, pd.DataFrame]:

    logger.info("Iniciando extração das APIs...")
    results = {}
    for name, url in ENDPOINTS.items():
        logger.info("Buscando %s ...", name)
        payload = _get_json_with_retries(url)
        if save_raw:
            try:
                _save_raw(name, payload)
            except Exception as e:
                logger.warning("Erro ao salvar raw %s: %s", name, e)

        try:
            df = pd.json_normalize(payload)
            
            if "id" not in df.columns and "ID" in df.columns:
                df = df.rename(columns={"ID": "id"})
            results[name] = df
            logger.info("Extraído %s: %d linhas, %d colunas", name, len(df), len(df.columns))
        except Exception as e:
            logger.error("Erro ao normalizar %s: %s", name, e)
            raise

    logger.info("Extração concluída.")
    return results


if __name__ == "__main__":
    dfs = extract_all(save_raw=True)
    for k, v in dfs.items():
        print(f"{k}: {v.shape}")