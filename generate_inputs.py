"""
generate_inputs.py
Run this FIRST to create all folders and sample flat files on C drive.
Usage: python generate_inputs.py
"""

import os
import json
import openpyxl

INPUT_DIR = r"C:\MedallionETL\input"
LOGS_DIR  = r"C:\MedallionETL\logs"

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR,  exist_ok=True)
print("Folders ready:")
print(f"  {INPUT_DIR}")
print(f"  {LOGS_DIR}")

# ── 1. orders.csv
orders_path = os.path.join(INPUT_DIR, "orders.csv")
with open(orders_path, "w", newline="") as f:
    f.write("order_id,customer_id,product_id,quantity,unit_price,order_date,status,discount_pct\n")
    f.write("1001,C001,P001,2,75000,2024-01-15,completed,5\n")
    f.write("1002,C002,P003,,450,2024-01-16,COMPLETED,0\n")
    f.write("1003,C003,P002,1,1200,16-01-2024,Completed,10\n")
    f.write("1004,,P004,1,15000,2024-01-17,pending,0\n")
    f.write("1005,C005,P001,1,75000,2024-01-18,cancelled,0\n")
    f.write("1006,C006,P005,3,2500,2024-01-19,completed,5\n")
    f.write("1007,C007,P003,5,450,2024-01-20,PENDING,0\n")
    f.write("1008,C008,P002,,1200,2024-01-21,completed,10\n")
    f.write("1009,C009,P004,2,15000,2024-01-22,completed,0\n")
    f.write("1010,C001,P001,1,75000,bad-date,completed,5\n")
    f.write("1011,C006,P005,4,2500,2024-01-23,Cancelled,0\n")
    f.write("1012,C002,P003,10,450,2024-01-24,completed,0\n")
    f.write("1013,C003,P002,2,1200,2024-01-25,completed,10\n")
    f.write("1014,C004,P004,1,15000,2024-01-26,COMPLETED,0\n")
    f.write("1015,C010,P001,3,75000,2024-01-27,pending,5\n")
print(f"Created: {orders_path}")

# ── 2. customers.json
customers = [
    {"customer_id": "C001", "full_name": "Alice Johnson",  "email": "alice@example.com",  "city": "Mumbai",    "signup_date": "2023-03-10", "tier": "Gold"},
    {"customer_id": "C002", "full_name": "bob smith",      "email": "BOB@EXAMPLE.COM",    "city": "delhi",     "signup_date": "2023-05-22", "tier": "silver"},
    {"customer_id": "C003", "full_name": "Carol White",    "email": "carol@example.com",  "city": "Bangalore", "signup_date": "2023-07-01", "tier": "Gold"},
    {"customer_id": "C004", "full_name": "",               "email": "dave@example.com",   "city": "Chennai",   "signup_date": "bad-date",   "tier": "Bronze"},
    {"customer_id": "C005", "full_name": "Eve Davis",      "email": "eve@example.com",    "city": "Hyderabad", "signup_date": "2023-08-14", "tier": "Bronze"},
    {"customer_id": "C006", "full_name": "Frank Miller",   "email": "frank@example.com",  "city": "Pune",      "signup_date": "2023-09-30", "tier": "Silver"},
    {"customer_id": "C007", "full_name": "Grace Lee",      "email": None,                 "city": "Kolkata",   "signup_date": "2023-11-05", "tier": "Gold"},
    {"customer_id": "C008", "full_name": "Hank Wilson",    "email": "hank@example.com",   "city": "Mumbai",    "signup_date": "2024-01-02", "tier": "Bronze"},
    {"customer_id": "C009", "full_name": "Ivy Moore",      "email": "ivy@example.com",    "city": "delhi",     "signup_date": "2024-01-10", "tier": "Silver"},
    {"customer_id": "C010", "full_name": "Jane Taylor",    "email": "jane@example.com",   "city": "Bangalore", "signup_date": "2024-01-15", "tier": "Gold"},
]
customers_path = os.path.join(INPUT_DIR, "customers.json")
with open(customers_path, "w") as f:
    json.dump(customers, f, indent=2)
print(f"Created: {customers_path}")

# ── 3. products.xlsx
wb = openpyxl.Workbook()
ws = wb.active
ws.title = "Products"
ws.append(["product_id", "product_name", "category", "cost_price",
           "sell_price", "stock_qty", "supplier", "is_active"])
product_rows = [
    ["P001", "Laptop",                      "Electronics", 55000, 75000,  50, "TechSupply Co", "Yes"],
    ["P002", "Mechanical Keyboard",         "Electronics",   700,  1200, 200, "KeyMaster Ltd", "Yes"],
    ["P003", "Wireless Mouse",              "Electronics",   200,   450, 350, "KeyMaster Ltd", "Yes"],
    ["P004", "4K Monitor",                  "Electronics",  9000, 15000,  80, "TechSupply Co", "Yes"],
    ["P005", "Noise Cancelling Headphones", "Electronics",  1200,  2500, 120, "AudioPro",      "Yes"],
    ["P006", "USB-C Hub",                   "Electronics",   600,  1100,   0, "TechSupply Co", "No"],
    ["P007", "Webcam HD",                   "Electronics",   800,  1800, None,"AudioPro",      "Yes"],
    ["P008", "Desk Lamp",                   "Accessories",   300,  None,  90, "HomeGear",      "Yes"],
    ["P009", "Laptop Stand",                "Accessories",   400,   850,  60, "HomeGear",      "Yes"],
    ["P010", "Cable Organiser",             "Accessories",    80,   200, 500, "HomeGear",      "Yes"],
]
for row in product_rows:
    ws.append(row)
products_path = os.path.join(INPUT_DIR, "products.xlsx")
wb.save(products_path)
print(f"Created: {products_path}")

# ── 4. returns.txt  (pipe-delimited)
returns_path = os.path.join(INPUT_DIR, "returns.txt")
with open(returns_path, "w", newline="") as f:
    f.write("return_id|order_id|product_id|return_date|reason|refund_amount|status\n")
    f.write("R001|1002|P003|2024-01-20|Defective|450|approved\n")
    f.write("R002|1005|P001|2024-01-22|Changed mind|75000|approved\n")
    f.write("R003|1008|P002|2024-01-25|Wrong item|1200|pending\n")
    f.write("R004|1011|P005|2024-01-26|Defective|2500|approved\n")
    f.write("R005|9999|P003|bad-date|Defective||pending\n")
    f.write("R006|1003|P002|2024-01-28|Not as described|1200|rejected\n")
    f.write("R007|1001|P001|2024-01-29|Defective|75000|pending\n")
print(f"Created: {returns_path}")

print("\nAll input files created successfully.")
print("Next step:  python etl_pipeline.py")
