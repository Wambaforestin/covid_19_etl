import pandas as pd
from pathlib import Path
from .base_transformer import BaseTransformer
from src.utils.logger import setup_logger

logger = setup_logger('cleaner')

class DataCleaner(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            logger.info("Début du nettoyage")
            
            processed_dir = Path(__file__).parent.parent.parent / 'data' / 'processed'
            processed_dir.mkdir(parents=True, exist_ok=True)
            # Supprimer les lignes où toutes les colonnes numériques sont à zéro
            numeric_columns = ['Confirmed', 'Deaths', 'Recovered', 'Active', 'New cases', 'New deaths', 'New recovered']
            df = df.drop(df[(df[numeric_columns] == 0).all(axis=1)].index)
            logger.info(f"Lignes avec toutes les valeurs numériques à zéro supprimées: {len(df)} lignes restantes")

            # 1. Supprimer les lignes avec plus de 50% de valeurs manquantes
            df = df.dropna(thresh=len(df.columns) * 0.5)

           # 2. Nettoyage par type de colonne
            for column in df.columns:
               if df[column].dtype == 'object':
                   # Nettoyage des chaînes
                   df[column] = df[column].str.strip().str.title()
               elif df[column].dtype in ['int64', 'float64']:
                   # Remplacer les valeurs négatives par 0
                   df[column] = df[column].clip(lower=0)

           # 3. Suppression des doublons
            initial_rows = len(df)
            df = self.remove_duplicates(df)
            duplicates_removed = initial_rows - len(df)
            logger.info(f"Doublons supprimés: {duplicates_removed}")

           # 4. Gestion des valeurs manquantes restantes
            numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
            text_columns = df.select_dtypes(include=['object']).columns

            df[numeric_columns] = df[numeric_columns].fillna(0)
            df[text_columns] = df[text_columns].fillna('Non renseigné')

            logger.info(f"Nettoyage terminé. Lignes restantes: {len(df)}")

            # Sauvegarde
            output_path = processed_dir / 'cleaned_data.csv'
            df.to_csv(output_path, index=False)
            logger.info(f"Données nettoyées sauvegardées: {output_path}")

            return df

        except Exception as e:
            logger.error(f"Erreur nettoyage: {str(e)}")
            raise