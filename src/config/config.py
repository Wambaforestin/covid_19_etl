import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

class Config:
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'covid_19_etl'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

    BASE_DIR = Path(__file__).parent.parent.parent
    DATA_PATHS = {
        'raw': BASE_DIR / 'data' / 'raw',
        'processed': BASE_DIR / 'data' / 'processed',
        'intermediate': BASE_DIR / 'data' / 'processed' / 'intermediate'
    }

    # Création des dossiers
    for path in DATA_PATHS.values():
        path.mkdir(parents=True, exist_ok=True)

    DATA_SOURCES = {
        'covid19': DATA_PATHS['raw'] / 'covid19_global_cases.csv',
    }

    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.DB_CONFIG['user']}:{self.DB_CONFIG['password']}@{self.DB_CONFIG['host']}:{self.DB_CONFIG['port']}/{self.DB_CONFIG['database']}"