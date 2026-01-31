import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

from prepare import prepare  # pipeline limpio (extract->transform->integrate->prepare)

# =========================
# CONFIG DB (compatible local + docker)
# =========================
DB_HOST = os.getenv("DW_HOST", "localhost")
DB_PORT = os.getenv("DW_PORT", "5432")
DB_NAME = os.getenv("DW_DB", "analisis_etl_dw")
DB_USER = os.getenv("DW_USER", "postgres")
DB_PASS = os.getenv("DW_PASSWORD", "666")


def get_engine():
    password = quote_plus(DB_PASS)
    url = f"postgresql+psycopg2://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def normalize_columns(dim_date, dim_product, dim_customer, dim_category, dim_channel, fact_sales):
    """
    Ajusta nombres de columnas para que coincidan con tus tablas reales en PostgreSQL.
    """

    # -------------------------
    # dim_customer (DB tiene: custnum, customer_name, channel)
    # Si el DF trae custnum_original o source_system, lo adaptamos.
    # -------------------------
    if "custnum_original" in dim_customer.columns and "custnum" not in dim_customer.columns:
        dim_customer = dim_customer.rename(columns={"custnum_original": "custnum"})

    # Si el DF trae source_system pero la tabla no existe en DB, lo quitamos
    if "source_system" in dim_customer.columns:
        dim_customer = dim_customer.drop(columns=["source_system"])

    # Si el DF NO trae channel, lo creamos (porque tu tabla sí lo tiene)
    if "channel" not in dim_customer.columns:
        dim_customer["channel"] = None

    # -------------------------
    # dim_category (DB tiene: category_name)
    # DF trae catalog_name -> renombrar
    # -------------------------
    if "catalog_name" in dim_category.columns and "category_name" not in dim_category.columns:
        dim_category = dim_category.rename(columns={"catalog_name": "category_name"})

    # -------------------------
    # dim_channel (DB suele ser: channel_key, channel_name)
    # Si tu DF trae otro nombre, ajusta aquí si aparece otro error.
    # -------------------------

    # -------------------------
    # fact_sales: asegurar que claves sean int donde corresponde
    # (esto evita inserts raros con floats)
    # -------------------------
    for col in ["date_key", "product_key", "customer_key", "category_key", "channel_key"]:
        if col in fact_sales.columns:
            fact_sales[col] = fact_sales[col].astype(int)

    return dim_date, dim_product, dim_customer, dim_category, dim_channel, fact_sales


def load():
    # 1) Generar DataFrames finales (limpios)
    dim_date, dim_product, dim_customer, dim_category, dim_channel, fact_sales = prepare()

    # 2) Normalizar nombres de columnas para DB real
    dim_date, dim_product, dim_customer, dim_category, dim_channel, fact_sales = normalize_columns(
        dim_date, dim_product, dim_customer, dim_category, dim_channel, fact_sales
    )

    # 3) Conectar
    engine = get_engine()

    # 4) Cargar (repetible)
    with engine.begin() as conn:
        print("\nConectado a PostgreSQL ✅")
        print(f"DB: {DB_NAME} | host: {DB_HOST}:{DB_PORT} | user: {DB_USER}")

        # Truncate seguro (solo si existen)
        conn.execute(text("""
            TRUNCATE TABLE
                fact_sales,
                dim_date,
                dim_product,
                dim_customer,
                dim_category,
                dim_channel
            RESTART IDENTITY CASCADE;
        """))

        # DIMs
        dim_date.to_sql("dim_date", conn, schema="public", if_exists="append", index=False)
        dim_product.to_sql("dim_product", conn, schema="public", if_exists="append", index=False)
        dim_customer.to_sql("dim_customer", conn, schema="public", if_exists="append", index=False)
        dim_category.to_sql("dim_category", conn, schema="public", if_exists="append", index=False)
        dim_channel.to_sql("dim_channel", conn, schema="public", if_exists="append", index=False)

        # FACT
        fact_sales.to_sql("fact_sales", conn, schema="public", if_exists="append", index=False)

    print("\n=== LOAD OK ✅ ===")
    print("dim_date:", dim_date.shape)
    print("dim_product:", dim_product.shape)
    print("dim_customer:", dim_customer.shape)
    print("dim_category:", dim_category.shape)
    print("dim_channel:", dim_channel.shape)
    print("fact_sales:", fact_sales.shape)


if __name__ == "__main__":
    load()
