import pandas as pd
from pathlib import Path

DW_PATH = Path("dw")
DW_PATH.mkdir(parents=True, exist_ok=True)
products = pd.read_csv("data/clean/products_clean.csv")
orders = pd.read_csv("data/clean/orders_clean.csv")

# DIM_PRODUCT
dim_product = products[[
    "product_id",
    "pcode",
    "description",
    "type",
    "supplier",
    "price",
    "cost"
]].drop_duplicates().sort_values("pcode").reset_index(drop=True)

dim_product.insert(0, "product_key", dim_product.index + 1)
dim_product.to_csv(DW_PATH / "dim_product.csv", index=False)

# DIM_CHANNEL
dim_channel = (
    orders[["source"]]
    .drop_duplicates()
    .sort_values("source")
    .reset_index(drop=True)
    .rename(columns={"source": "channel_name"})
)

dim_channel.insert(0, "channel_key", dim_channel.index + 1)
dim_channel.to_csv(DW_PATH / "dim_channel.csv", index=False)

# DIM_CUSTOMER
dim_customer = (
    orders[["custnum"]]
    .drop_duplicates()
    .sort_values("custnum")
    .reset_index(drop=True)
    .rename(columns={"custnum": "customer_id"})
)

dim_customer.insert(0, "customer_key", dim_customer.index + 1)
dim_customer.to_csv(DW_PATH / "dim_customer.csv", index=False)

# DIM_TIME
orders["date"] = pd.to_datetime(orders["date"], errors="coerce", dayfirst=True)

dim_time = (
    orders[["date"]]
    .drop_duplicates()
    .dropna()
    .sort_values("date")
    .reset_index(drop=True)
)

dim_time["day"] = dim_time["date"].dt.day
dim_time["month"] = dim_time["date"].dt.month
dim_time["year"] = dim_time["date"].dt.year

dim_time.insert(0, "time_key", dim_time.index + 1)
dim_time.to_csv(DW_PATH / "dim_time.csv", index=False)

# FACT_SALES
fact_sales = orders.copy()

# Join con dimensiones
fact_sales = fact_sales.merge(
    dim_product[["product_key", "pcode", "price", "cost"]],
    on="pcode",
    how="left"
)

fact_sales = fact_sales.merge(
    dim_channel,
    left_on="source",
    right_on="channel_name",
    how="left"
)

fact_sales = fact_sales.merge(
    dim_customer,
    left_on="custnum",
    right_on="customer_id",
    how="left"
)

fact_sales = fact_sales.merge(
    dim_time[["time_key", "date"]],
    on="date",
    how="left"
)

# Métricas
fact_sales["sales_amount"] = fact_sales["qty"] * fact_sales["price"]
fact_sales["cost_amount"] = fact_sales["qty"] * fact_sales["cost"]
fact_sales["margin"] = fact_sales["sales_amount"] - fact_sales["cost_amount"]

# Selección final de columnas
fact_sales = fact_sales[[
    "product_key",
    "channel_key",
    "customer_key",
    "time_key",
    "qty",
    "sales_amount",
    "cost_amount",
    "margin"
]]

fact_sales.to_csv(DW_PATH / "fact_sales.csv", index=False)