import pandas as pd
from pathlib import Path
from .base_transformer import BaseTransformer
from src.utils.logger import setup_logger

logger = setup_logger('aggregator')

class DataAggregator(BaseTransformer):
    def transform(self, df: pd.DataFrame, pays_df: pd.DataFrame, maladie_df: pd.DataFrame) -> pd.DataFrame:
        try:
            logger.info("Début agrégation")

            processed_dir = Path(__file__).parent.parent.parent / 'data' / 'processed'
            processed_dir.mkdir(parents=True, exist_ok=True)

            logger.info("Début agrégation des données")

            # Jointure avec les tables de référence
            df = df.merge(pays_df[['nom_pays', 'id_pays']], 
                          on='nom_pays', 
                          how='left',
                          validate='m:1')

            df = df.merge(maladie_df[['nom_maladie', 'id_maladie']], 
                          on='nom_maladie',
                          how='left',
                          validate='m:1')

            # Vérification des jointures
            missing_pays = df[df['id_pays'].isna()]['nom_pays'].unique()
            missing_maladies = df[df['id_maladie'].isna()]['nom_maladie'].unique()

            if len(missing_pays) > 0:
                logger.warning(f"Pays non trouvés: {missing_pays}")
            if len(missing_maladies) > 0:
                logger.warning(f"Maladies non trouvées: {missing_maladies}")

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

            # Vérification des résultats
            logger.info(f"Nombre de lignes agrégées: {len(aggregated)}")
            logger.info(f"Nombre de pays uniques: {aggregated['id_pays'].nunique()}")
            logger.info(f"Nombre de maladies uniques: {aggregated['id_maladie'].nunique()}")

            # Sauvegarde
            output_path = processed_dir / 'aggregated_data.csv'
            aggregated.to_csv(output_path, index=False)
            logger.info(f"Données agrégées sauvegardées: {output_path}")

            return aggregated

        except Exception as e:
            logger.error(f"Erreur agrégation: {str(e)}")
            raise

    def validate_aggregation(self, df: pd.DataFrame) -> bool:
        """Valide les résultats de l'agrégation"""
        try:
            # Vérification des valeurs négatives
            numeric_cols = ['cas_confirmes', 'deces', 'guerisons', 'cas_actifs',
                            'nouveaux_cas', 'nouveaux_deces', 'nouvelles_guerisons']

            for col in numeric_cols:
                if (df[col] < 0).any():
                    logger.error(f"Valeurs négatives trouvées dans {col}")
                    return False

            # Vérification de la cohérence des données
            if not (df['cas_actifs'] == df['cas_confirmes'] - df['deces'] - df['guerisons']).all():
                logger.error("Incohérence dans le calcul des cas actifs")
                return False

            return True

        except Exception as e:
            logger.error(f"Erreur lors de la validation: {str(e)}")
            return False
