import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

class Config:
    # Configuration Base de données
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'covid_19_etl'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

    # Configuration des chemins
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_PATHS = {
        'raw': os.path.join(BASE_DIR, 'data', 'raw'),
        'processed': os.path.join(BASE_DIR, 'data', 'processed'),
        'intermediate': os.path.join(BASE_DIR, 'data', 'processed', 'intermediate')
    }

    def __init__(self):
        # Création des dossiers nécessaires
        for path in self.DATA_PATHS.values():
            os.makedirs(path, exist_ok=True)

    @property
    def DATA_SOURCES(self):
        return {
            'covid19': os.path.join(self.DATA_PATHS['raw'], 'covid19_global_cases.csv')
        }

    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.DB_CONFIG['user']}:{self.DB_CONFIG['password']}@{self.DB_CONFIG['host']}:{self.DB_CONFIG['port']}/{self.DB_CONFIG['database']}"