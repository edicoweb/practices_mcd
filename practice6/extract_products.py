import pandas as pd

def extract_products():
    products_raw = pd.read_csv(
        "data/products.csv",
        engine="python",
        on_bad_lines="skip"
    )

    products = products_raw.iloc[:, 0].str.split(";", expand=True)
    products.columns = [
        "product_id",
        "type",
        "description",
        "price",
        "cost",
        "pcode",
        "supplier"
    ]

    products["price"] = pd.to_numeric(products["price"], errors="coerce")
    products["cost"] = pd.to_numeric(products["cost"], errors="coerce")
    products["pcode"] = products["pcode"].str.strip()

    products.to_csv("data/clean1/products_c.csv", index=False)

if __name__ == "__main__":
    extract_products()




