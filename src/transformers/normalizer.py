import pandas as pd
from .base_transformer import BaseTransformer
from src.utils.logger import setup_logger

logger = setup_logger('normalizer')

class DataNormalizer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            logger.info("Début normalisation")
            
            # Standardisation des noms de pays
            df['pays'] = df['pays'].str.strip().str.title()
            
            # Conversion des dates
            df['date_observation'] = pd.to_datetime(df['date_observation'])
            
            # Normalisation des valeurs numériques
            numeric_columns = ['cas_confirmes', 'deces', 'guerisons', 'cas_actifs']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
            return df

        except Exception as e:
            logger.error(f"Erreur normalisation: {str(e)}")
            raise