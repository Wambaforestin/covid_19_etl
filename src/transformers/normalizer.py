import pandas as pd
import os
from .base_transformer import BaseTransformer
from src.utils.logger import setup_logger
from src.config import Config

logger = setup_logger('normalizer')
config = Config()

class DataNormalizer(BaseTransformer):
  def transform(self, df: pd.DataFrame) -> pd.DataFrame:
   try:
       logger.info("Début normalisation")
       # Log des pays avant toute transformation
       logger.info(f"Liste des pays dans le CSV brut: {sorted(df['Country/Region'].unique())}")
       logger.info(f"Colonnes d'entrée: {df.columns.tolist()}")
       
       # Renommage des colonnes
       column_mapping = {
           'Country/Region': 'nom_pays',
           'Confirmed': 'cas_confirmes',
           'Deaths': 'deces',
           'Recovered': 'guerisons',
           'Active': 'cas_actifs',
           'New cases': 'nouveaux_cas',
           'New deaths': 'nouveaux_deces',
           'New recovered': 'nouvelles_guerisons',
           'Date': 'date_observation',
           'WHO Region': 'region_oms'
       }
       df = df.rename(columns=column_mapping)
       logger.info(f"Colonnes après renommage: {df.columns.tolist()}")
       
       # Standardisation des noms de pays
       df['nom_pays'] = df['nom_pays'].str.strip()
       
       # Mapping spécifique des noms de pays
       pays_mapping = {
    'Us': 'US',
    "Cote D'Ivoire": "Cote d'Ivoire", 
    'West Bank And Gaza': 'West Bank and Gaza',
    'Antigua And Barbuda': 'Antigua and Barbuda',
    'Bosnia And Herzegovina': 'Bosnia and Herzegovina',
    'Saint Kitts And Nevis': 'Saint Kitts and Nevis',
    'Saint Vincent And The Grenadines': 'Saint Vincent and the Grenadines',
    'Sao Tome And Principe': 'Sao Tome and Principe',
    'Trinidad And Tobago': 'Trinidad and Tobago'
}
       df['nom_pays'] = df['nom_pays'].replace(pays_mapping)
       
       # Conversion de la date
       df['date_observation'] = pd.to_datetime(df['date_observation'])
       
       # Nettoyage des valeurs numériques
       numeric_columns = ['cas_confirmes', 'deces', 'guerisons', 'cas_actifs',
                        'nouveaux_cas', 'nouveaux_deces', 'nouvelles_guerisons']
       
       for col in numeric_columns:
           if col in df.columns:
               df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
           else:
               logger.warning(f"Colonne {col} manquante, création avec valeurs à 0")
               df[col] = 0
       
       # Ajout de nom_maladie pour la jointure future
       df['nom_maladie'] = 'COVID-19'
       
       # Vérification finale des données
       logger.info(f"Nombre de lignes normalisées: {len(df)}")
       logger.info(f"Nombre de pays uniques: {df['nom_pays'].nunique()}")
       logger.info(f"Plage de dates: {df['date_observation'].min()} à {df['date_observation'].max()}")
       
       return df
       
   except Exception as e:
       logger.error(f"Erreur normalisation: {str(e)}")
       raise