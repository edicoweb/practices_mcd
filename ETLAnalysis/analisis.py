import pandas as pd

catalog = pd.read_csv("Catalog_Orders.txt", sep=",")
catalog["QTY_num"] = pd.to_numeric(catalog["QTY"], errors="coerce")

print("=== CATALOG_ORDERS (3.iv) ===")

print("\nCantidad de registros por CATALOG:")
print(catalog["CATALOG"].value_counts().head(15))

print("\nPromedio de QTY por CATALOG:")
print(catalog.groupby("CATALOG")["QTY_num"].mean().sort_values(ascending=False).head(15))

print("\nTotal de QTY por CATALOG:")
print(catalog.groupby("CATALOG")["QTY_num"].sum().sort_values(ascending=False).head(15))

print("\nClientes (custnum) con más compras (Top 10):")
print(catalog["custnum"].value_counts().head(10))

print("\nProductos (PCODE) con más compras (Top 10):")
print(catalog["PCODE"].value_counts().head(10))




