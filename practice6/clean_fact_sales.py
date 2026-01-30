import pandas as pd
from pathlib import Path

DW_PATH = Path("dw")
input_file = DW_PATH / "fact_sales.csv"

fact_sales = pd.read_csv(input_file)
fact_sales_clean = fact_sales.dropna(subset=[
    "product_key",
    "channel_key",
    "customer_key",
    "time_key"
])

output_file = DW_PATH / "fact_sales_clean.csv"
fact_sales_clean.to_csv(output_file, index=False)

