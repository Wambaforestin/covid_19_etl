import unittest
import pandas as pd
from src.transformers.aggregator import DataAggregator
from src.transformers.cleaner import DataCleaner
from src.transformers.normalizer import DataNormalizer
from src.loaders.postgres_loader import PostgresLoader
from unittest.mock import patch, MagicMock

class TestDataAggregator(unittest.TestCase):
    def setUp(self):
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
        self.pays_df = pd.DataFrame({
            'nom_pays': ['Country1', 'Country2'],
            'id_pays': [1, 2]
        })

    def test_transform(self):
        result = self.aggregator.transform(self.df, self.pays_df)
        self.assertEqual(len(result), 2)
        self.assertIn('id_pays', result.columns)
        self.assertIn('id_maladie', result.columns)

class TestDataCleaner(unittest.TestCase):
    def setUp(self):
        self.cleaner = DataCleaner()
        self.df = pd.DataFrame({
            'col1': [1, 2, None, 4],
            'col2': ['a', 'b', 'c', None],
            'col3': [None, None, None, None]
        })

    def test_transform(self):
        result = self.cleaner.transform(self.df)
        self.assertEqual(len(result), 3)
        self.assertNotIn(None, result['col1'])
        self.assertNotIn(None, result['col2'])

class TestDataNormalizer(unittest.TestCase):
    def setUp(self):
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
        self.assertIn('nom_pays', result.columns)
        self.assertIn('cas_confirmes', result.columns)
        self.assertIn('date_observation', result.columns)

class TestPostgresLoader(unittest.TestCase):
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
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__.return_value = mock_session
        loader.bulk_insert('situation_pandemique', df)
        mock_session.connection().execute.assert_called()

if __name__ == '__main__':
    unittest.main()