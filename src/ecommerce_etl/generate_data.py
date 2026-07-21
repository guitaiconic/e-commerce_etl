"""
Simulates raw e-commerce order data landing from a source system.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker("en_GB")

PRODUCTS = [
    ("Wireless Mouse", 19.99),
    ("Mechanical Keyboard", 79.99),
    ("USB-C Hub", 34.50),
    ("Laptop Stand", 45.00),
    ("Webcam 1080p", 59.99),
    ("Noise Cancelling Headphones", 149.99),
    ("Desk Lamp", 24.99),
    ("Monitor 27-inch", 189.00),
    ("Bluetooth Speaker", 39.99),
    ("Phone Charger Cable", 9.99),
]

CHANNELS = ["web", "mobile_app", "marketplace"]


def generate_orders(num_rows: int = 5000, output_path: str = "data/raw/orders.csv") -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    start_date = datetime(2025, 1, 1)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_id", "customer_email", "product_name", "unit_price",
            "quantity", "order_date", "channel", "country"
        ])

        for i in range(1, num_rows + 1):
            product_name, unit_price = random.choice(PRODUCTS)
            order_date = start_date + timedelta(days=random.randint(0, 200))

            email = fake.email() if random.random() > 0.03 else None
            quantity = random.randint(1, 5) if random.random() > 0.02 else -1

            writer.writerow([
                i,
                email,
                product_name,
                unit_price,
                quantity,
                order_date.strftime("%Y-%m-%d"),
                random.choice(CHANNELS),
                fake.country() if random.random() > 0.9 else "United Kingdom",
            ])

    print(f"Generated {num_rows} rows -> {output_path}")


if __name__ == "__main__":
    generate_orders()
