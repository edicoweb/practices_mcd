import pandas as pd
from sqlalchemy import create_engine

# CONEXIÓN A POSTGRESQL
user = "postgres"
password = "sdcd23dsds"
host = "localhost"
port = "5432"
db = "DBsales"

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
)

# CARGA DE ARCHIVOS DW
dim_product = pd.read_csv("dw/dim_product.csv")
dim_channel = pd.read_csv("dw/dim_channel.csv")
dim_customer = pd.read_csv("dw/dim_customer.csv")
dim_time = pd.read_csv("dw/dim_time.csv")
fact_sales = pd.read_csv("dw/fact_sales_clean.csv")

# VALIDACIONES BÁSICAS
assert fact_sales[[
    "product_key",
    "channel_key",
    "customer_key",
    "time_key"
]].notnull().all().all(), "Hay claves nulas en fact_sales"

assert (fact_sales["qty"] > 0).all(), "Hay cantidades inválidas"

# RENOMBRE PARA POSTGRES
fact_sales = fact_sales.rename(columns={
    "qty": "quantity",
    "sales_amount": "revenue",
    "cost_amount": "total_cost"
})

# MÉTRICA DERIVADA
fact_sales["profit"] = (
    fact_sales["revenue"] - fact_sales["total_cost"]
)

# ELIMINAR PK SERIAL (CLAVE)
dim_product_db  = dim_product.drop(columns=["product_key"], errors="ignore")
dim_channel_db  = dim_channel.drop(columns=["channel_key"], errors="ignore")
dim_customer_db = dim_customer.drop(columns=["customer_key"], errors="ignore")
dim_time_db     = dim_time.drop(columns=["time_key"], errors="ignore")

# CARGA A POSTGRESQL
dim_product_db.to_sql(
    "dim_product",
    engine,
    if_exists="append",
    index=False
)

dim_channel_db.to_sql(
    "dim_channel",
    engine,
    if_exists="append",
    index=False
)

dim_customer_db.to_sql(
    "dim_customer",
    engine,
    if_exists="append",
    index=False
)

dim_time_db.to_sql(
    "dim_time",
    engine,
    if_exists="append",
    index=False
)

fact_sales.to_sql(
    "fact_sales",
    engine,
    if_exists="append",
    index=False
)

print("CARGA OK")
