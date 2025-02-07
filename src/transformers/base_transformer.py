from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def handle_missing_values(self, df: pd.DataFrame, strategy='drop') -> pd.DataFrame:
        '''Gestion des valeurs manquantes
            si strategy='drop', on supprime les lignes avec valeurs manquantes
            si strategy='fill_zero', on remplace les valeurs manquantes par 0
            si strategy='fill_mean', on remplace les valeurs manquantes par la moyenne
            si strategy='fill_median', on remplace les valeurs manquantes par la médiane
        '''
        if strategy == 'drop':
            return df.dropna()
        elif strategy == 'fill_zero':
            return df.fillna(0)
        elif strategy == 'fill_mean':
            return df.fillna(df.mean())
        elif strategy == 'fill_median':
            return df.fillna(df.median())
        else:
            raise ValueError(f"Stratégie {strategy} non supportée")

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates()