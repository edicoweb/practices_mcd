import pandas as pd
from transform import transform


def integrate():
    catalog, web, products = transform()

    # 1) Crear columna CHANNEL
    catalog["CHANNEL"] = "Catalog"
    web["CHANNEL"] = "Web"

    # 2) Unir pedidos (staging orders)
    orders = pd.concat([catalog, web], ignore_index=True)

    # 3) Asegurar tipos numéricos
    orders["INV"] = pd.to_numeric(orders["INV"], errors="coerce")
    orders["QTY"] = pd.to_numeric(orders["QTY"], errors="coerce")

    # 4) Crear dimensiones

    # DIM_PRODUCT
    dim_product = products.copy()
    dim_product = dim_product.rename(columns={
        "ID": "product_key",
        "TYPE": "type",
        "DESCRIP": "descrip",
        "PRICE": "price",
        "COST": "cost",
        "PCODE": "pcode",
        "supplier": "supplier"
    })[["product_key", "pcode", "descrip", "type", "supplier", "price", "cost"]]

    # DIM_CATEGORY
    dim_category = pd.DataFrame({"catalog_name": sorted(orders["CATALOG"].dropna().unique())})
    dim_category["category_key"] = range(1, len(dim_category) + 1)
    dim_category = dim_category[["category_key", "catalog_name"]]

    # DIM_CHANNEL
    dim_channel = pd.DataFrame({"channel_name": sorted(orders["CHANNEL"].unique())})
    dim_channel["channel_key"] = range(1, len(dim_channel) + 1)
    dim_channel = dim_channel[["channel_key", "channel_name"]]

    # DIM_CUSTOMER (FIX DEFINITIVO)
    orders["custnum"] = orders["custnum"].fillna("UNKNOWN").astype(str).str.strip()

    cust_list = orders["custnum"].dropna().astype(str).unique().tolist()
    cust_list = sorted(cust_list)

    dim_customer = pd.DataFrame({"custnum_original": cust_list})
    dim_customer["customer_key"] = range(1, len(dim_customer) + 1)
    dim_customer["customer_name"] = dim_customer["custnum_original"]
    dim_customer["source_system"] = "Mixed"
    dim_customer = dim_customer[["customer_key", "custnum_original", "customer_name", "source_system"]]

    # DIM_DATE
    dim_date = pd.DataFrame({"full_date": sorted(orders["DATE"].dropna().dt.date.unique())})
    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])
    dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["day"] = dim_date["full_date"].dt.day.astype(int)
    dim_date["month"] = dim_date["full_date"].dt.month.astype(int)
    dim_date["year"] = dim_date["full_date"].dt.year.astype(int)
    dim_date = dim_date[["date_key", "full_date", "day", "month", "year"]]

    # 5) FACT_SALES

    # Renombrar PCODE -> pcode
    orders = orders.rename(columns={"PCODE": "pcode"})
    orders["pcode"] = orders["pcode"].astype(str).str.strip()

    # Merge con dim_product
    orders = orders.merge(dim_product[["product_key", "pcode", "price", "cost"]], on="pcode", how="left")

    # Merge con dim_category
    orders = orders.merge(dim_category, left_on="CATALOG", right_on="catalog_name", how="left")

    # Merge con dim_channel
    orders = orders.merge(dim_channel, left_on="CHANNEL", right_on="channel_name", how="left")

    # Merge con dim_customer
    orders = orders.merge(dim_customer, left_on="custnum", right_on="custnum_original", how="left")

    # date_key
    orders["date_key"] = orders["DATE"].dt.strftime("%Y%m%d").astype(int)

    # Métricas
    orders["sales_amount"] = orders["QTY"] * orders["price"]
    orders["cost_amount"] = orders["QTY"] * orders["cost"]
    orders["margin_amount"] = orders["sales_amount"] - orders["cost_amount"]

    # Fact final
    fact_sales = pd.DataFrame({
        "fact_id": range(1, len(orders) + 1),
        "date_key": orders["date_key"],
        "product_key": orders["product_key"],
        "customer_key": orders["customer_key"],
        "category_key": orders["category_key"],
        "channel_key": orders["channel_key"],
        "inv": orders["INV"],
        "qty": orders["QTY"],
        "sales_amount": orders["sales_amount"],
        "cost_amount": orders["cost_amount"],
        "margin_amount": orders["margin_amount"]
    })

    # Reporte
    print("\n=== INTEGRATION REPORT ===")
    print("dim_product:", dim_product.shape)
    print("dim_category:", dim_category.shape)
    print("dim_channel:", dim_channel.shape)
    print("dim_customer:", dim_customer.shape)
    print("dim_date:", dim_date.shape)
    print("fact_sales:", fact_sales.shape)

    print("\nFact_sales nulos (por columna):")
    print(fact_sales.isna().sum())
    
    print("\nPrimeras filas de fact_sales:")
    print(fact_sales.head())
    return dim_date, dim_product, dim_customer, dim_category, dim_channel, fact_sales


if __name__ == "__main__":
    integrate()
