import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    base_dir: Path = BASE_DIR
    data_dir: Path = BASE_DIR / "data"
    raw_dir: Path = BASE_DIR / "data" / "raw"
    raw_kamis_dir: Path = BASE_DIR / "data" / "raw" / "kamis"
    raw_knbs_dir: Path = BASE_DIR / "data" / "raw" / "knbs"
    raw_operational_dir: Path = BASE_DIR / "data" / "raw" / "operational"
    bronze_dir: Path = BASE_DIR / "data" / "bronze"
    silver_dir: Path = BASE_DIR / "data" / "silver"
    gold_dir: Path = BASE_DIR / "data" / "gold"

    # Kafka Topic Configuration
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "retail-market-group")
    topic_pos_sales: str = "retail.pos.transactions"
    topic_inventory: str = "retail.inventory.movements"
    topic_price_alerts: str = "market.price.alerts"

    # Database Configuration
    database_url: str = ""

    def get_database_url(self) -> str:
        if self.database_url:
            return self._normalize_db_url(self.database_url)

        env_db_url = os.getenv("DATABASE_URL")
        if env_db_url:
            return self._normalize_db_url(env_db_url)

        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD", "")
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME")

        if db_user and db_name:
            return f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        db_path = (self.base_dir / "retail_intelligence.db").as_posix()
        return f"sqlite:///{db_path}"

    @staticmethod
    def _normalize_db_url(url: str) -> str:
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return url


settings = Settings()
