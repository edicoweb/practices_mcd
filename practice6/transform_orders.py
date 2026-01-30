
import pandas as pd

def transform_orders():
    web = pd.read_csv("data/web_orders.csv", sep=";")
    catalog = pd.read_csv("data/catalog_orders.csv", sep=";")

    web = web.rename(columns={
        "ID": "id",
        "INV": "inv",
        "DATE": "date",
        "CATALOG": "catalog",
        "PCODE": "pcode",
        "QTY": "qty",
        "custnum": "custnum"
    })

    catalog = catalog.rename(columns={
        "id": "id",
        "inv": "inv",
        "date": "date",
        "catalog": "catalog",
        "pcode": "pcode",
        "qty": "qty",
        "custnum": "custnum"
    })

    web["source"] = "web"
    catalog["source"] = "catalog"

    orders = pd.concat([web, catalog], ignore_index=True)

    orders["pcode"] = orders["pcode"].str.strip()
    orders["qty"] = pd.to_numeric(orders["qty"], errors="coerce")
    orders["date"] = pd.to_datetime(orders["date"], dayfirst=True, errors="coerce")

    orders.to_csv("data/clean1/orders_u.csv", index=False)

if __name__ == "__main__":
    transform_orders()
