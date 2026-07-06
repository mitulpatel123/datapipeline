"""Phase 1b checkpoint: create all tables from storage.postgres_models."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from storage.postgres_client import engine
from storage.postgres_models import Base

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    tables = sorted(Base.metadata.tables.keys())
    print(f"Created/verified {len(tables)} tables:")
    for t in tables:
        print(f"  - {t}")
