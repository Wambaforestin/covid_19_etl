import os
from dotenv import load_dotenv
from pathlib import Path

# Charger les variables d'environnement
load_dotenv()

class Config:
    # Configuration Base de données
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'covid_19_etl'),  # Nom actualisé
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

    # Configuration des chemins avec Path
    BASE_DIR = Path(__file__).parent.parent.parent
    DATA_PATHS = {
        'raw': BASE_DIR / 'data' / 'raw',
        'processed': BASE_DIR / 'data' / 'processed',
    }

    # Sources de données
    DATA_SOURCES = {
        'covid19': DATA_PATHS['raw'] / 'covid19_global_cases.csv',
    }

    # URL de connexion SQLAlchemy
    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.DB_CONFIG['user']}:{self.DB_CONFIG['password']}@{self.DB_CONFIG['host']}:{self.DB_CONFIG['port']}/{self.DB_CONFIG['database']}"