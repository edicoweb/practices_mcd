import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


def extract():
    print("Leyendo archivos desde:", DATA_DIR)

    # 1) Catalog Orders
    catalog_path = DATA_DIR / "Catalog_Orders.txt"
    catalog = pd.read_csv(catalog_path)

    # 2) Web Orders (sin header, con líneas corruptas omitidas)
    web_path = DATA_DIR / "Web_orders.txt"
    web = pd.read_csv(
        web_path,
        sep=";",
        engine="python",
        header=None,
        on_bad_lines="skip"
    )
    web.columns = ["ID", "INV", "PCODE", "DATE", "CATALOG", "QTY", "custnum"]

    # 3) Products
    products_path_lower = DATA_DIR / "products.txt"
    products_path_upper = DATA_DIR / "Products.txt"

    if products_path_lower.exists():
        products = pd.read_csv(products_path_lower)
    else:
        products = pd.read_csv(products_path_upper)

    # Mostrar resultados
    print("\n=== CATALOG ===")
    print("shape:", catalog.shape)
    print("cols:", list(catalog.columns))
    print(catalog.head(), "\n")

    print("=== WEB ===")
    print("shape:", web.shape)
    print("cols:", list(web.columns))
    print(web.head(), "\n")

    print("=== PRODUCTS ===")
    print("shape:", products.shape)
    print("cols:", list(products.columns))
    print(products.head(), "\n")

    return catalog, web, products


if __name__ == "__main__":
    extract()
