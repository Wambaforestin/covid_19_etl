import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
        output_path = config.DATA_PATHS['intermediate'] / f"{stage}_data.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Données intermédiaires sauvegardées: {output_path}")

    def initialize_reference_data(self):
        try:
            with self.db_manager.get_session() as session:
                covid = Maladie(
                    nom_maladie='COVID-19',
                    type_maladie='Virale',
                    description='Maladie infectieuse causée par le SARS-CoV-2'
                )
                session.add(covid)
                session.commit()
                logger.info("Données de référence initialisées")
        except Exception as e:
            logger.error(f"Erreur initialisation données: {str(e)}")
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
            cleaned_df = self.cleaner.transform(df)
            self.save_intermediate(cleaned_df, 'cleaned')

            normalized_df = self.normalizer.transform(cleaned_df)
            self.save_intermediate(normalized_df, 'normalized')

            aggregated_df = self.aggregator.transform(normalized_df)
            self.save_intermediate(aggregated_df, 'aggregated')

            # Sauvegarde finale
            final_path = config.DATA_PATHS['processed'] / 'covid19_processed.csv'
            aggregated_df.to_csv(final_path, index=False)
            logger.info(f"Données finales sauvegardées: {final_path}")

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
            self.initialize_reference_data()
            df = self.extract()
            transformed_df = self.transform(df)
            self.load(transformed_df)
            logger.info("Pipeline ETL terminé")
            return True
        except Exception as e:
            logger.error(f"Erreur pipeline: {str(e)}")
            return False

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()