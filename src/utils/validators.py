import pandas as pd
from datetime import datetime

class DataValidator:
    @staticmethod
    def validate_date(date_str):
        try:
            return pd.to_datetime(date_str)
        except:
            return None

    @staticmethod
    def validate_numeric(value):
        try:
            return int(value)
        except:
            return 0

    @staticmethod
    def validate_string(value, max_length=None):
        if pd.isna(value): # isna est utilisé pour vérifier les valeurs manquantes
            return None
        value = str(value).strip()
        return value[:max_length] if max_length else value