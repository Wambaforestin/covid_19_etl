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

    def load_reference_data(self, pays_df: pd.DataFrame, maladie_df: pd.DataFrame):
        self.bulk_insert('pays', pays_df, if_exists='replace')
        self.bulk_insert('maladie', maladie_df, if_exists='replace')

    def load_situation_data(self, situation_df: pd.DataFrame):
        self.bulk_insert('situation_pandemique', situation_df, if_exists='append')