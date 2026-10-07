# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import csv, random
from datetime import date, timedelta
import os

random.seed(42)                       # same "random" data every time you run it

BASE = "/Volumes/learning/bronze/raw"

os.makedirs(f"{BASE}/customers", exist_ok=True)
os.makedirs(f"{BASE}/products", exist_ok=True)

# ---------- CUSTOMERS ----------
countries = ["Germany", "France", "Netherlands", "Spain", "Italy", "Poland", "Austria", "Sweden"]
first = ["Anna", "Lukas", "Sara", "Jonas", "Mia", "Leon", "Emma", "Paul", "Lea", "Ali", "Nora", "Max"]
last  = ["Schmidt", "Müller", "Rossi", "Garcia", "Novak", "Berg", "Dubois", "Weber", "Jansen", "Kaya"]

customers = []
for cid in range(1, 201):            # 200 customers, ids 1..200
    signup = date(2025, 1, 1) + timedelta(days=random.randint(0, 600))
    customers.append([cid, f"{random.choice(first)} {random.choice(last)}",
                      random.choice(countries), signup.isoformat()])

# --- intentional problems ---
customers[10][2] = None              # missing country
customers[20][3] = "31/02/2025"      # malformed / impossible date
customers.append(list(customers[5])) # exact duplicate row

with open(f"{BASE}/customers/customers.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["customer_id", "name", "country", "signup_date"])
    w.writerows(customers)

# ---------- PRODUCTS ----------
catalog = {
    "Electronics": ["Headphones", "Keyboard", "Mouse", "Monitor", "Webcam", "Speaker"],
    "Home":        ["Lamp", "Chair", "Desk", "Pillow", "Mug", "Clock"],
    "Sports":      ["Yoga Mat", "Dumbbell", "Bottle", "Backpack", "Running Shoes", "Helmet"],
    "Books":       ["Novel", "Cookbook", "Biography", "Comic", "Atlas", "Dictionary"],
    "Beauty":      ["Shampoo", "Perfume", "Cream", "Brush", "Soap", "Lotion"],
}

products, price_by_id, pid = [], {}, 1
for category, names in catalog.items():
    for name in names:               # 30 products, ids 1..30
        price = round(random.uniform(5, 300), 2)
        products.append([pid, name, category, price])
        price_by_id[pid] = price
        pid += 1

# --- intentional problems ---
products[3][3]  = -10.0              # invalid negative price
products[15][3] = None               # missing price

with open(f"{BASE}/products/products.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["product_id", "product_name", "category", "price"])
    w.writerows(products)

print(f"customers: {len(customers)} rows | products: {len(products)} rows")

# COMMAND ----------

def write_orders(file_date: str, start_id: int, n: int):
    """Simulates one daily export of orders from the shop system."""
    rows = []
    for i in range(n):
        product_id = random.randint(1, 30)
        hh, mm, ss = random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        rows.append([
            start_id + i,                                     # order_id
            random.randint(1, 200),                           # customer_id
            product_id,
            random.randint(1, 5),                             # quantity
            price_by_id[product_id],                          # unit_price
            f"{file_date} {hh:02d}:{mm:02d}:{ss:02d}",        # order_timestamp
            random.choice(["completed", "pending", "shipped", "cancelled"]),
        ])

    # --- intentional problems ---
    rows[10][1] = None                    # missing customer_id
    rows[11][1] = None
    rows[12][1] = 9999                    # customer that doesn't exist
    rows[13][3] = -5                      # negative quantity
    rows[14][3] = 0                       # zero quantity
    rows[15][5] = "not_a_date"            # malformed timestamp
    rows[16][5] = "2026-13-45 25:61:00"   # impossible timestamp
    rows[20][6] = "Completed"             # inconsistent status spelling
    rows[21][6] = " shipped "
    rows[22][6] = "PENDING"
    rows += [list(r) for r in rows[30:35]]  # 5 exact duplicate rows

    path = f"{BASE}/orders/orders_{file_date.replace('-', '_')}.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "product_id", "quantity",
                    "unit_price", "order_timestamp", "status"])
        w.writerows(rows)
    print(f"wrote {len(rows)} rows -> {path}")

write_orders("2026-10-01", start_id=1001, n=500)

# COMMAND ----------

display(dbutils.fs.ls(f"{BASE}/orders"))
print(open(f"{BASE}/orders/orders_2026_10_01.csv").read()[:600])