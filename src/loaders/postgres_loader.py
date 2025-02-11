from sqlalchemy.exc import SQLAlchemyError
from src.config import db_manager
from src.utils.logger import setup_logger
import pandas as pd

logger = setup_logger('postgres_loader')

class PostgresLoader:
    def __init__(self):
        self.db = db_manager

    def bulk_insert(self, table_name: str, df: pd.DataFrame, if_exists='append'):
        try:
            with self.db.get_session() as session:
                df.to_sql(
                    name=table_name,
                    con=session.connection(),
                    if_exists=if_exists,
                    index=False,
                    chunksize=1000
                )
                logger.info(f"Chargement réussi dans {table_name}: {len(df)} lignes")
        except SQLAlchemyError as e:
            logger.error(f"Erreur chargement {table_name}: {str(e)}")
            raise

    def load_situation_data(self, df: pd.DataFrame):
        try:
            # Vérifier que nous avons les bonnes colonnes
            required_columns = ['id_pays', 'id_maladie', 'date_observation', 
                              'cas_confirmes', 'deces', 'guerisons', 'cas_actifs',
                              'nouveaux_cas', 'nouveaux_deces', 'nouvelles_guerisons']
            
            if not all(col in df.columns for col in required_columns):
                logger.error(f"Colonnes manquantes. Attendues: {required_columns}")
                logger.error(f"Reçues: {df.columns.tolist()}")
                raise ValueError("Colonnes manquantes pour situation_pandemique")
            
            # Ne sélectionner que les colonnes nécessaires dans le bon ordre
            situation_df = df[required_columns]
            
            self.bulk_insert('situation_pandemique', situation_df, if_exists='append')
            
        except Exception as e:
            logger.error(f"Erreur chargement situation_pandemique: {str(e)}")
            raise