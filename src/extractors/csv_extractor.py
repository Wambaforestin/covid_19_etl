import pandas as pd
from src.utils.logger import setup_logger
from src.config import Config

logger = setup_logger('csv_extractor')
config = Config()

class CSVExtractor:
   def __init__(self, filepath):
       self.filepath = filepath

   def extract(self):
       try:
           logger.info(f"Début extraction CSV: {self.filepath}")
           df = pd.read_csv(self.filepath)
           logger.info(f"Extraction réussie: {len(df)} lignes")
           
           # Profil des données
           profile = {
               "nombre_lignes": len(df),
               "nombre_colonnes": len(df.columns),
               "valeurs_manquantes": df.isnull().sum().to_dict(),
               "doublons": df.duplicated().sum(),
               "types_donnees": df.dtypes.to_dict(),
               "colonnes": list(df.columns),
               "memoire_utilisee": f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB",
               "apercu_donnees": df.head().to_dict()
           }
           
           logger.info(f"Profil des données:\n{profile}")
           return df, profile
           
       except Exception as e:
           logger.error(f"Erreur extraction CSV: {str(e)}")
           raise

   def validate_columns(self, required_columns):
       df, profile = self.extract()
       missing_columns = set(required_columns) - set(df.columns)
       if missing_columns:
           raise ValueError(f"Colonnes manquantes: {missing_columns}")
       return df, profile