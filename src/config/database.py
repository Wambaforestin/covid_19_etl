from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from .config import Config
from src.utils.logger import setup_logger

logger = setup_logger('database')
config = Config()

class DatabaseManager:
    def __init__(self):
        self.engine = None
        self.session_maker = None
        self.metadata = MetaData()

    def connect(self):
        try:
            self.engine = create_engine(
                config.DATABASE_URL,
                pool_size=20,
                max_overflow=0,
                pool_timeout=30,
                pool_recycle=1800
            )
            self.session_maker = sessionmaker(bind=self.engine)
            logger.info("Connexion BD établie")
        except SQLAlchemyError as e:
            logger.error(f"Erreur connexion BD: {str(e)}")
            raise

    @contextmanager
    def get_session(self):
        if not self.session_maker:
            self.connect()
        session = self.session_maker()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Erreur session: {str(e)}")
            raise
        finally:
            session.close()

    def execute_query(self, query, params=None):
        with self.get_session() as session:
            return session.execute(query, params if params else {})

db_manager = DatabaseManager()