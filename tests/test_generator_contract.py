from pathlib import Path

import numpy as np
import pandas as pd

from src.generate_data import generate_customers, generate_products


def test_dimension_generators(tmp_path: Path):
    rng = np.random.default_rng(42)
    generate_customers(rng, 10, tmp_path)
    generate_products(rng, 5, tmp_path)

    customers = pd.read_csv(tmp_path / "customers.csv")
    products = pd.read_csv(tmp_path / "products.csv")

    assert len(customers) == 10
    assert customers["customer_id"].is_unique
    assert len(products) == 5
    assert products["product_id"].is_unique
    assert (products["unit_price"] > 0).all()
