from pathlib import Path
import pytest
import pandas as pd
from src.transformers.aggregator import DataAggregator
from src.transformers.cleaner import DataCleaner
from src.transformers.normalizer import DataNormalizer
from src.loaders.postgres_loader import PostgresLoader
from unittest.mock import patch, MagicMock


# Test de la classe DataAggregator
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
        assert 'id_pays' in result.columns  # Ensure the country ID is added
        assert 'id_maladie' in result.columns  # Ensure the disease ID is added


# Test de la classe DataCleaner
# class TestDataCleaner:
#     def setup_method(self):
#         self.cleaner = DataCleaner()
#         # Create a sample DataFrame with the expected columns
#         self.df = pd.DataFrame({
#             'Confirmed': [0, 100, 0, 50, 0],
#             'Deaths': [0, 10, 0, 5, 0],
#             'Recovered': [0, 90, 0, 45, 0],
#             'Active': [0, 0, 0, 0, 0],
#             'New cases': [0, 5, 0, 2, 0],
#             'New deaths': [0, 1, 0, 0, 0],
#             'New recovered': [0, 4, 0, 2, 0],
#             'Country': ['USA', 'Canada', '  france  ', 'Germany', 'Non renseigné'],
#             'Region': ['North America', 'North America', 'Europe', 'Europe', None]
#         })

#     def test_transform(self):
#         result = self.cleaner.transform(self.df)

#         # Assertions to verify the transformation
#         # 1. Rows with all numeric columns as 0 should be removed
#         assert len(result) == 2  # Only 2 rows should remain after removing rows with all zeros

#         # 2. Strings should be stripped and title-cased
#         assert result['Country'].tolist() == ['USA', 'Canada', 'France', 'Germany', 'Non Renseigné']

#         # 3. Negative values in numeric columns should be replaced with 0
#         assert (result.select_dtypes(include=['int64', 'float64']) >= 0).all().all()

#         # 4. Duplicates should be removed
#         assert result.duplicated().sum() == 0

#         # 5. Missing values in text columns should be filled with 'Non renseigné'
#         assert result['Region'].isna().sum() == 0
#         assert result['Region'].tolist() == ['North America', 'North America', 'Europe', 'Europe', 'Non renseigné']

#         # 6. Verify the output file is saved (optional, if you want to test file saving)
#         output_path = Path(__file__).parent.parent.parent / 'data' / 'processed' / 'cleaned_data.csv'
#         assert output_path.exists()


# Test de la classe DataNormalizer
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


# Test de la classe PostgresLoader
# class TestPostgresLoader:
#     @patch('pandas.DataFrame.to_sql')
#     @patch('src.config.db_manager.get_session')
#     def test_bulk_insert(self, mock_get_session, mock_to_sql):
#         loader = PostgresLoader()
#         df = pd.DataFrame({
#             'id_pays': [1, 2],
#             'id_maladie': [1, 1],
#             'date_observation': ['2021-01-01', '2021-01-02'],
#             'cas_confirmes': [100, 200],
#             'deces': [10, 20],
#             'guerisons': [50, 100],
#             'cas_actifs': [40, 80],
#             'nouveaux_cas': [5, 10],
#             'nouveaux_deces': [1, 2],
#             'nouvelles_guerisons': [3, 6]
#         })
#         df["date_observation"] = pd.to_datetime(df["date_observation"]).dt.date  # Fix date format

#         mock_session = MagicMock()
#         mock_get_session.return_value.__enter__.return_value = mock_session

#         # Mock the engine and connection
#         mock_engine = MagicMock()
#         mock_session.bind = mock_engine

#         loader.bulk_insert('situation_pandemique', df)

#         # Verify that to_sql was called with the correct arguments
#         mock_to_sql.assert_called_once_with(
#             name='situation_pandemique',
#             con=mock_engine.connect.return_value.__enter__.return_value,
#             if_exists='append',
#             index=False
#         )