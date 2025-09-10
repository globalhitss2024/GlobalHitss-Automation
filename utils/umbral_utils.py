# umbral_utils.py
import numpy as np
import pandas as pd

class UmbralEvaluator:
    """
    Clase para evaluar y asignar la columna 'Umbral' en un DataFrame.
    
    Reglas:
      - ESTRATEGICAS: recurrente_fo_hfc >= 1,000,000 y cfm_movil >= 1,000,000
      - GRANDES: recurrente_fo_hfc >= 500,000 y cfm_movil >= 500,000
      - Negocios (campo 'segmento'): recurrente_fo_hfc >= 1 y cfm_movil >= 1
      - De lo contrario: NO APLICA
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def _normalize_columns(self):
        """Normaliza columnas de texto y numéricas necesarias."""
        self.df['segmento_actual'] = self.df['segmento_actual'].astype(str).str.strip().str.upper()
        self.df['segmento'] = self.df['segmento'].astype(str).str.strip().str.upper()

        for col in ['recurrente_fo_hfc', 'cfm_movil']:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
            else:
                self.df[col] = 0

    def apply_rules(self) -> pd.DataFrame:
        """Aplica las reglas de negocio y retorna el DataFrame con la columna 'Umbral'."""
        self._normalize_columns()

        cond_estrategicas = (
            (self.df['segmento_actual'] == 'ESTRATEGICAS') &
            (self.df['recurrente_fo_hfc'] >= 1_000_000) &
            (self.df['cfm_movil'] >= 1_000_000)
        )

        cond_grandes = (
            (self.df['segmento_actual'] == 'GRANDES') &
            (self.df['recurrente_fo_hfc'] >= 500_000) &
            (self.df['cfm_movil'] >= 500_000)
        )

        cond_negocios = (
            (self.df['segmento'] == 'NEGOCIOS') &
            (self.df['recurrente_fo_hfc'] >= 1) &
            (self.df['cfm_movil'] >= 1)
        )

        condiciones = [cond_estrategicas, cond_grandes, cond_negocios]
        valores = ['APLICA', 'APLICA', 'APLICA']

        self.df['umbral'] = np.select(condiciones, valores, default='NO APLICA')

        return self.df