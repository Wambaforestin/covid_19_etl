from pathlib import Path
import pytest
import pandas as pd
from src.transformers.aggregator import DataAggregator
from src.transformers.cleaner import DataCleaner
from src.transformers.normalizer import DataNormalizer
from src.loaders.postgres_loader import PostgresLoader
from unittest.mock import patch, MagicMock

# test de la classe DataAggregator
class TestDataAggregator:
    def setup_method(self):
        self.aggregator = DataAggregator()
        self.df = pd.DataFrame({
            'nom_pays': ['Country1', 'Country2'],
            'cas_confirmes': [100, 200],
            'deces': [10, 20],
            'guerisons': [50, 100],
            'cas_actifs': [40, 80],
            'nouveaux_cas': [5, 10],
            'nouveaux_deces': [1, 2],
            'nouvelles_guerisons': [3, 6],
            'date_observation': ['2021-01-01', '2021-01-02']
        })
        self.df["date_observation"] = pd.to_datetime(self.df["date_observation"]).dt.date  # Fix date format
        self.pays_df = pd.DataFrame({
            'nom_pays': ['Country1', 'Country2'],
            'id_pays': [1, 2]
        })

    def test_transform(self):
        result = self.aggregator.transform(self.df, self.pays_df)
        assert len(result) == 2
        assert 'id_pays' in result.columns # ce rassure que l'ID du pays est bien ajouté
        assert 'id_maladie' in result.columns  # ce rassure que l'ID de la maladie est bien ajouté
        
# test de la classe DataCleaner
class TestDataCleaner:
    def setup_method(self):
        self.cleaner = DataCleaner()
        # Create a sample DataFrame with some edge cases
        self.df = pd.DataFrame({
            'Confirmed': [0, 100, 0, 50, 0],
            'Deaths': [0, 10, 0, 5, 0],
            'Recovered': [0, 90, 0, 45, 0],
            'Active': [0, 0, 0, 0, 0],
            'New cases': [0, 5, 0, 2, 0],
            'New deaths': [0, 1, 0, 0, 0],
            'New recovered': [0, 4, 0, 2, 0],
            'Country': ['USA', 'Canada', '  france  ', 'Germany', 'Non renseigné'],
            'Region': ['North America', 'North America', 'Europe', 'Europe', None]
        })

    def test_transform(self):
        result = self.cleaner.transform(self.df)

        # Assertions to verify the transformation
        # 1. Rows with all numeric columns as 0 should be removed
        assert len(result) == 3  # Only 3 rows should remain after removing rows with all zeros

        # 2. Strings should be stripped and title-cased
        assert result['Country'].tolist() == ['USA', 'Canada', 'France', 'Germany', 'Non Renseigné']

        # 3. Negative values in numeric columns should be replaced with 0
        assert (result.select_dtypes(include=['int64', 'float64']) >= 0).all().all()

        # 4. Duplicates should be removed
        assert result.duplicated().sum() == 0

        # 5. Missing values in numeric columns should be filled with 0
        assert result['Region'].isna().sum() == 0  # Missing values in text columns should be filled with 'Non renseigné'
        assert result['Region'].tolist() == ['North America', 'North America', 'Europe', 'Europe', 'Non renseigné']

        # 6. Missing values in text columns should be filled with 'Non renseigné'
        assert result['Region'].isna().sum() == 0

        # 7. Verify the output file is saved (optional, if you want to test file saving)
        output_path = Path(__file__).parent.parent.parent / 'data' / 'processed' / 'cleaned_data.csv'
        assert output_path.exists()
        
class TestDataNormalizer:
    def setup_method(self):
        self.normalizer = DataNormalizer()
        self.df = pd.DataFrame({
            'Country/Region': ['Country1', 'Country2'],
            'Confirmed': [100, 200],
            'Deaths': [10, 20],
            'Recovered': [50, 100],
            'Active': [40, 80],
            'New cases': [5, 10],
            'New deaths': [1, 2],
            'New recovered': [3, 6],
            'Date': ['2021-01-01', '2021-01-02'],
            'WHO Region': ['Region1', 'Region2']
        })

    def test_transform(self):
        result = self.normalizer.transform(self.df)
        assert 'nom_pays' in result.columns
        assert 'cas_confirmes' in result.columns
        assert 'date_observation' in result.columns

class TestPostgresLoader:
    @patch('src.config.db_manager.get_session')
    def test_bulk_insert(self, mock_get_session):
        loader = PostgresLoader()
        df = pd.DataFrame({
            'id_pays': [1, 2],
            'id_maladie': [1, 1],
            'date_observation': ['2021-01-01', '2021-01-02'],
            'cas_confirmes': [100, 200],
            'deces': [10, 20],
            'guerisons': [50, 100],
            'cas_actifs': [40, 80],
            'nouveaux_cas': [5, 10],
            'nouveaux_deces': [1, 2],
            'nouvelles_guerisons': [3, 6]
        })
        df["date_observation"] = pd.to_datetime(df["date_observation"]).dt.date  # Fix date format

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__.return_value = mock_session
        loader.bulk_insert('situation_pandemique', df)

        # Ensure bulk insert is called correctly
        assert mock_session.connection().execute.called or mock_session.add_all.called
        
        
