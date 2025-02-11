import os
import sys
import pandas as pd
from pathlib import Path

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.config import Config
from src.config.database import db_manager
from src.extractors.csv_extractor import CSVExtractor
from src.transformers import DataCleaner, DataAggregator, DataNormalizer
from src.loaders.postgres_loader import PostgresLoader
from src.utils.logger import setup_logger
from src.models import Pays, Maladie, SituationPandemique

logger = setup_logger('etl_main')
config = Config()

class ETLPipeline:
   def __init__(self):
       self.db_manager = db_manager
       self.db_manager.connect()
       self.loader = PostgresLoader()
       self.cleaner = DataCleaner()
       self.normalizer = DataNormalizer()
       self.aggregator = DataAggregator()

   def save_intermediate(self, df, stage):
       output_path = os.path.join(config.DATA_PATHS['intermediate'], f"{stage}_data.csv")
       df.to_csv(output_path, index=False)
       logger.info(f"Données intermédiaires sauvegardées: {output_path}")

   def initialize_pays(self, df):
    """Initialise la table pays avec normalisation des noms"""
    try:
        with self.db_manager.get_session() as session:
            # Vérifier si la table pays est vide
            count = session.query(Pays).count()
            if count == 0:
                # Extraire les pays uniques
                pays_uniques = df[['Country/Region', 'WHO Region']].drop_duplicates()
                
                # Normalisation des noms de pays
                pays_df = pd.DataFrame({
                    'nom_pays': pays_uniques['Country/Region'].apply(lambda x: x.strip().title().replace(' And ', ' and ')),
                    'region_oms': pays_uniques['WHO Region']
                })
                
                # Cas spéciaux
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
                
                pays_df['nom_pays'] = pays_df['nom_pays'].replace(pays_mapping)
                
                # Charger les pays
                self.loader.bulk_insert('pays', pays_df, if_exists='append')
                logger.info(f"{len(pays_df)} pays initialisés")
            else:
                logger.info(f"Table pays déjà initialisée avec {count} pays")
            
    except Exception as e:
        logger.error(f"Erreur initialisation pays: {str(e)}")
        raise

   def get_reference_data(self):
       try:
           with self.db_manager.get_session() as session:
               pays_query = session.query(Pays).all()
               pays_df = pd.DataFrame([
                   {'nom_pays': p.nom_pays, 'id_pays': p.id_pays}
                   for p in pays_query
               ])
               logger.info(f"Données pays récupérées: {len(pays_df)} pays")
               return pays_df
           
       except Exception as e:
           logger.error(f"Erreur récupération données référence: {str(e)}")
           raise

   def initialize_reference_data(self):
       try:
           with self.db_manager.get_session() as session:
               # Vérifier si COVID-19 existe déjà
               existing_covid = session.query(Maladie).filter_by(nom_maladie='COVID-19').first()
               
               if not existing_covid:
                   covid = Maladie(
                       nom_maladie='COVID-19',
                       type_maladie='Virale',
                       description='Maladie infectieuse causée par le SARS-CoV-2'
                   )
                   session.add(covid)
                   session.commit()
                   logger.info("Maladie COVID-19 initialisée")
               else:
                   logger.info("La maladie COVID-19 existe déjà")
       except Exception as e:
           logger.error(f"Erreur initialisation maladie: {str(e)}")
           raise

   def extract(self):
       try:
           extractor = CSVExtractor(config.DATA_SOURCES['covid19'])
           df, profile = extractor.extract()
           logger.info(f"Extraction terminée: {profile}")
           return df
       except Exception as e:
           logger.error(f"Erreur extraction: {str(e)}")
           raise

   def transform(self, df):
       try:
           # 1. Nettoyage
           cleaned_df = self.cleaner.transform(df)
           logger.info("Nettoyage terminé")

           # 2. Normalisation
           normalized_df = self.normalizer.transform(cleaned_df)
           logger.info("Normalisation terminée")

           # 3. Obtention des données de référence
           pays_df = self.get_reference_data()
           logger.info("Données de référence récupérées")

           # 4. Agrégation 
           aggregated_df = self.aggregator.transform(normalized_df, pays_df)
           logger.info("Agrégation terminée")

           return aggregated_df
           
       except Exception as e:
           logger.error(f"Erreur transformation: {str(e)}")
           raise

   def load(self, df):
       try:
           self.loader.load_situation_data(df)
           logger.info("Chargement terminé")
       except Exception as e:
           logger.error(f"Erreur chargement: {str(e)}")
           raise

   def run(self):
       try:
           logger.info("Démarrage pipeline ETL")
           
           # 1. Extraction initiale
           df = self.extract()
           
           # 2. Initialisation des données de référence
           self.initialize_reference_data()  # Initialise COVID-19
           self.initialize_pays(df)          # Initialise les pays
           
           # 3. Transformation
           transformed_df = self.transform(df)
           
           # 4. Chargement
           self.load(transformed_df)
           
           logger.info("Pipeline ETL terminé")
           return True
       except Exception as e:
           logger.error(f"Erreur pipeline: {str(e)}")
           return False

if __name__ == "__main__":
   pipeline = ETLPipeline()
   pipeline.run()