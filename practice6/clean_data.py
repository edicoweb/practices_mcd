import pandas as pd
from pathlib import Path

DATA_CLEAN = Path("data/clean")
DATA_CLEAN.mkdir(parents=True, exist_ok=True)

# CARGA DE ARCHIVOS
orders = pd.read_csv("data/clean1/orders_u.csv")
products = pd.read_csv("data/clean1/products_c.csv")

# LIMPIEZA DEL ARCHIVO ORDERS
#Fecha
orders["date"] = pd.to_datetime(orders["date"], errors="coerce")
orders = orders.dropna(subset=["date"])

#QTY
orders["qty"] = pd.to_numeric(orders["qty"], errors="coerce")
orders = orders[orders["qty"] > 0]
orders["qty"] = orders["qty"].astype(int)

#Source o Canal
orders["source"] = (
    orders["source"]
    .str.strip()
    .str.lower()
    .replace({
        "web": "Web",
        "catalog": "Catalog"
    })
)

#Catalog
orders["catalog"] = (
    orders["catalog"]
    .str.strip()
    .str.title()
    .replace({
        "Pet": "Pets"
    })
)

#CORRECCIÓN PCODE
orders["pcode"] = orders["pcode"].replace({
    "PT2OOO": "PT2000"
})

#ELIMINACIÓN DE DUPLICADOS
orders = orders.drop_duplicates()


# LIMPIEZA DE PRODUCTS
products = products.drop_duplicates(subset=["pcode"])

products["price"] = pd.to_numeric(products["price"], errors="coerce")
products["cost"] = pd.to_numeric(products["cost"], errors="coerce")

products = products.dropna(subset=["price", "cost"])

# NORMALIZACIÓN DE CATALOG
catalog_map = {
    # Toys
    "Toy": "Toys",
    "Tosy": "Toys",

    # Gardening
    "Garden": "Gardening",
    "Gardenings": "Gardening",

    # Sports
    "Sport": "Sports",
    "Sporst": "Sports",
    "Spots": "Sports",

    # Collectibles
    "Collectible": "Collectibles",
    "Colectibles": "Collectibles",

    # Software
    "Softwar": "Software",
    "Softwars": "Software",
    "Softwares": "Software",

    # Pets
    "Pats": "Pets",
    "Pest": "Pets"
}

orders["catalog"] = orders["catalog"].replace(catalog_map)

# GUARDADO DE ARCHIVOS LIMPIOS
orders.to_csv(DATA_CLEAN / "orders_clean.csv", index=False)
products.to_csv(DATA_CLEAN / "products_clean.csv", index=False)

print("ok")
