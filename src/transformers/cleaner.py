import pandas as pd
from .base_transformer import BaseTransformer
from src.utils.logger import setup_logger
import os
import config

logger = setup_logger('cleaner')

class DataCleaner(BaseTransformer):
   def transform(self, df: pd.DataFrame) -> pd.DataFrame:
       """
       Nettoie les données en appliquant les règles suivantes :
       - Suppression des lignes avec trop de valeurs manquantes
       - Correction des types de données
       - Suppression des doublons
       - Nettoyage des chaînes de caractères
       """
       try:
           logger.info("Début du nettoyage")

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
           
           df.to_csv(os.path.join(config.DATA_PATHS['processed'], 
                 'cleaned_data.csv'), index=False)
           
           return df

       except Exception as e:
           logger.error(f"Erreur lors du nettoyage: {str(e)}")
           raise