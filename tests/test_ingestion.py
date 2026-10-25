from pathlib import Path
import pandas as pd
import pytest
from src.ingestion.kamis import ingest_kamis_data, generate_kamis_dataset, compute_file_sha256
from src.ingestion.knbs import ingest_knbs_data, generate_knbs_dataset
from src.ingestion.operational_generator import ingest_operational_data, generate_operational_datasets


def test_kamis_generator():
    df = generate_kamis_dataset(2023, 2024)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2400
    assert "wholesale_price_kes" in df.columns
    assert "retail_price_kes" in df.columns
    assert "market_name" in df.columns


def test_knbs_generator():
    df = generate_knbs_dataset(2023, 2024)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 24
    assert "cpi_overall" in df.columns
    assert "cpi_food" in df.columns


def test_operational_generator():
    stores, supp, prod, pos, inv = generate_operational_datasets(num_transactions=100, num_inventory_events=50)
    assert len(stores) == 15
    assert len(supp) == 20
    assert len(prod) == 25
    assert len(pos) == 100
    assert len(inv) == 50


def test_ingest_kamis_data(tmp_path: Path):
    path, meta = ingest_kamis_data(output_dir=tmp_path, force=True)
    assert path.exists()
    assert meta["rows"] == 2400
    assert len(meta["checksum_sha256"]) == 64


def test_ingest_knbs_data(tmp_path: Path):
    path, meta = ingest_knbs_data(output_dir=tmp_path, force=True)
    assert path.exists()
    assert meta["rows"] == 24
    assert len(meta["checksum_sha256"]) == 64
