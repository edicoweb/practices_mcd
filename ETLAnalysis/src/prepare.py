import pandas as pd
from integrate import integrate


def prepare():
    dim_date, dim_product, dim_customer, dim_category, dim_channel, fact_sales = integrate()

    # =========================
    # 1) Agregar "Unknown Product" a dim_product
    # =========================
    unknown_product = pd.DataFrame([{
        "product_key": 0,
        "pcode": "UNKNOWN",
        "descrip": "Unknown Product",
        "type": "Unknown",
        "supplier": "Unknown",
        "price": 0.0,
        "cost": 0.0
    }])

    if not (dim_product["product_key"] == 0).any():
        dim_product = pd.concat([unknown_product, dim_product], ignore_index=True)

    # =========================
    # 2) Agregar "Unknown Category" a dim_category
    # =========================
    unknown_category = pd.DataFrame([{
        "category_key": 0,
        "catalog_name": "Unknown"
    }])

    if not (dim_category["category_key"] == 0).any():
        dim_category = pd.concat([unknown_category, dim_category], ignore_index=True)

    # =========================
    # 3) Normalizar claves en fact_sales
    # =========================
    fact_sales["product_key"] = fact_sales["product_key"].fillna(0).astype(int)
    fact_sales["category_key"] = fact_sales["category_key"].fillna(0).astype(int)

    # =========================
    # 4) Normalizar qty y eliminar inválidos
    # =========================
    before = len(fact_sales)

    fact_sales["qty"] = pd.to_numeric(fact_sales["qty"], errors="coerce")
    fact_sales = fact_sales.dropna(subset=["qty"])
    fact_sales = fact_sales[fact_sales["qty"] > 0]

    # CLAVE: resetear índice para evitar NaN por alineación
    fact_sales = fact_sales.reset_index(drop=True)

    after = len(fact_sales)

    # =========================
    # 5) Recalcular métricas SIN NaN
    # =========================
    temp = fact_sales.merge(
        dim_product[["product_key", "price", "cost"]],
        on="product_key",
        how="left"
    )

    temp["price"] = pd.to_numeric(temp["price"], errors="coerce").fillna(0)
    temp["cost"] = pd.to_numeric(temp["cost"], errors="coerce").fillna(0)

    temp["sales_amount"] = temp["qty"] * temp["price"]
    temp["cost_amount"] = temp["qty"] * temp["cost"]
    temp["margin_amount"] = temp["sales_amount"] - temp["cost_amount"]

    # Asignación por posición (no por índice) para evitar NaNs
    fact_sales["sales_amount"] = pd.to_numeric(temp["sales_amount"], errors="coerce").fillna(0).to_numpy()
    fact_sales["cost_amount"] = pd.to_numeric(temp["cost_amount"], errors="coerce").fillna(0).to_numpy()
    fact_sales["margin_amount"] = pd.to_numeric(temp["margin_amount"], errors="coerce").fillna(0).to_numpy()

    # =========================
    # 6) Reporte final
    # =========================
    print("\n=== PREPARE REPORT ===")
    print("Filas eliminadas por qty inválido:", before - after)
    print("fact_sales:", fact_sales.shape)
    print("\nNulos finales en fact_sales:")
    print(fact_sales.isna().sum())

    return dim_date, dim_product, dim_customer, dim_category, dim_channel, fact_sales


if __name__ == "__main__":
    prepare()
