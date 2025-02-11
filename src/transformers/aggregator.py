import pandas as pd
from pathlib import Path
from .base_transformer import BaseTransformer
from src.utils.logger import setup_logger

logger = setup_logger('aggregator')

class DataAggregator(BaseTransformer):
    def transform(self, df: pd.DataFrame, pays_df: pd.DataFrame) -> pd.DataFrame:
        try:
            logger.info("Début agrégation")
            
            # Log détaillé des données entrantes
            logger.info(f"Colonnes DataFrame entrée: {df.columns.tolist()}")
            logger.info(f"Colonnes pays_df: {pays_df.columns.tolist()}")
            logger.info(f"Noms de pays dans le DataFrame: {sorted(df['nom_pays'].unique())}")
            logger.info(f"Noms de pays dans la base de données: {sorted(pays_df['nom_pays'].unique())}")
            
            # Jointure pour obtenir id_pays
            df = df.merge(pays_df[['nom_pays', 'id_pays']], 
                        on='nom_pays', 
                        how='left')

            # Vérification détaillée de la jointure
            missing_pays = df[df['id_pays'].isna()]['nom_pays'].unique()
            if len(missing_pays) > 0:
                logger.error("Détails des pays manquants:")
                for pays in missing_pays:
                    logger.error(f"'{pays}' n'a pas été trouvé")
                    similaires = [p for p in pays_df['nom_pays'] if p.lower().replace(' ', '') in pays.lower().replace(' ', '') 
                                or pays.lower().replace(' ', '') in p.lower().replace(' ', '')]
                    if similaires:
                        logger.error(f"Noms similaires trouvés dans la base: {similaires}")
                raise ValueError("Certains pays n'ont pas été trouvés dans la table de référence")

            # Ajout de l'id_maladie pour COVID-19
            df['id_maladie'] = 1

            # Agrégation par ID
            aggregated = df.groupby(['id_pays', 'id_maladie', 'date_observation']).agg({
                'cas_confirmes': 'sum',
                'deces': 'sum',
                'guerisons': 'sum',
                'cas_actifs': 'sum',
                'nouveaux_cas': 'sum',
                'nouveaux_deces': 'sum',
                'nouvelles_guerisons': 'sum'
            }).reset_index()

            logger.info(f"Agrégation terminée: {len(aggregated)} lignes")
            return aggregated

        except Exception as e:
            logger.error(f"Erreur agrégation: {str(e)}")
            raise