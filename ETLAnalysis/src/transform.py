import pandas as pd
from extract import extract


def clean_pcode(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.upper()

    # Arreglar confusión O -> 0
    # Ejemplo: PT2OOO -> PT2000
    s = s.str.replace("O", "0", regex=False)

    return s


def clean_catalog(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.title()

    # Correcciones típicas encontradas en tus datos
    mapping = {
        "Sport": "Sports",
        "Sporst": "Sports",
        "Spots": "Sports",

        "Toy": "Toys",
        "Tosy": "Toys",
        "Tots": "Toys",

        "Pet": "Pets",
        "Pest": "Pets",
        "Pats": "Pets",
        "Prts": "Pets",

        "Softwares": "Software",
        "Softwars": "Software",
        "Softwar": "Software",

        "Collectable": "Collectibles",
        "Collectables": "Collectibles",
        "Collectible": "Collectibles",
        "Colectibles": "Collectibles",

        # Errores detectados en tu salida
        "Gardning": "Gardening",
        "Gardenings": "Gardening",
        "Garden": "Gardening",
    }

    s = s.replace(mapping)

    # limpiar valores corruptos raros que aparecieron
    s = s.astype(str).str.replace('"', '', regex=False)
    s = s.astype(str).str.replace(",", "", regex=False)

    # si quedó algo como Ty4400 (basura), lo mandamos a Toys
    s = s.replace({"Ty4400": "Toys"})

    return s


def transform():
    # 1) Extraer DataFrames desde extract.py
    catalog, web, products = extract()

    # 2) Limpieza de PCODE
    catalog["PCODE"] = clean_pcode(catalog["PCODE"])
    web["PCODE"] = clean_pcode(web["PCODE"])
    products["PCODE"] = clean_pcode(products["PCODE"])

    # 3) Limpieza de CATALOG
    catalog["CATALOG"] = clean_catalog(catalog["CATALOG"])
    web["CATALOG"] = clean_catalog(web["CATALOG"])

    # 4) QTY a numérico
    catalog["QTY"] = pd.to_numeric(catalog["QTY"], errors="coerce")
    web["QTY"] = pd.to_numeric(web["QTY"], errors="coerce")

    # 5) Fechas a datetime
    # Web: día/mes/año
    web["DATE"] = pd.to_datetime(web["DATE"], dayfirst=True, errors="coerce")

    # Catalog: formato raro tipo 3/97/7 00:00:00 (mes/año/día)
    catalog_date = catalog["DATE"].astype(str).str.split(" ", expand=True)[0]
    parts = catalog_date.str.split("/", expand=True)

    catalog["month"] = pd.to_numeric(parts[0], errors="coerce")
    catalog["year"] = pd.to_numeric(parts[1], errors="coerce") + 1900
    catalog["day"] = pd.to_numeric(parts[2], errors="coerce")

    catalog["DATE"] = pd.to_datetime(
        dict(year=catalog["year"], month=catalog["month"], day=catalog["day"]),
        errors="coerce"
    )

    # 6) Reporte rápido (para el informe)
    print("\n=== TRANSFORM REPORT ===")

    print("\nCATALOG únicos (Catalog_Orders):")
    print(catalog["CATALOG"].value_counts().head(15))

    print("\nCATALOG únicos (Web_Orders):")
    print(web["CATALOG"].value_counts().head(15))

    print("\nNulos después de limpieza:")
    print("Catalog QTY nulos:", catalog["QTY"].isna().sum())
    print("Web QTY nulos:", web["QTY"].isna().sum())
    print("Catalog DATE nulos:", catalog["DATE"].isna().sum())
    print("Web DATE nulos:", web["DATE"].isna().sum())

    return catalog, web, products


if __name__ == "__main__":
    transform()
