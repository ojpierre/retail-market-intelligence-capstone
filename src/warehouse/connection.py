import logging
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

try:
    from ..config import settings
except ImportError:
    from config import settings

logger = logging.getLogger(__name__)


def get_engine(database_url: Optional[str] = None) -> Engine:
    url = database_url or settings.get_database_url()
    return create_engine(url, pool_pre_ping=True)
