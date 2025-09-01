'''
PROYECTO:           UTILIDADES DE TRANSFORMACIÓN DE DATOS
AUTOR:              GEORGE GALINDO
OPERACIÓN:          MÓDULO CONTIENE CLASES Y FUNCIONES DE APOYO PARA LIMPIEZA Y TRANSFORMACIÓN
VERSIÓN:            V_1.0
FECHA:              18/08/2025
DESCRIPCIÓN:        CLASES Y MÉTODOS PARA LIMPIAR TEXTO, CLASIFICAR PRODUCTOS, 
                    PARSEAR MONEDAS Y CREAR TABLAS RESUMEN.
ACTUALIZACIÓN:      SE ORGANIZA EN ESTRUCTURA DE CLASES ORIENTADAS A OBJETOS
'''

import pandas as pd
import unicodedata
import re
from decimal import Decimal, InvalidOperation


# Clase utilitaria con todos los métodos de transformación
class DataUtils:
    def __init__(self):
        # Definir caracteres especiales que representan "menos"
        self.MINUS_CHARS = ['-', '–', '—', '−']

    # ---- Métodos de texto ----
    def limpiar_texto(self, texto):
        """Normaliza texto: quita tildes, convierte a mayúscula y elimina nulos."""
        if pd.isnull(texto):
            return ''
        texto = str(texto).strip().upper()
        texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
        return texto

    def clasificar_producto(self, producto):
        """Clasifica productos en categorías predefinidas."""
        producto = str(producto).lower()
        if 'television' in producto or 'tv' in producto:
            return 'TV'
        elif 'internet' in producto:
            return 'INTERNET'
        elif 'voz' in producto:
            return 'VOZ'
        elif 'datos' in producto:
            return 'DATOS'
        else:
            return 'E-BUSINESS'

    # ---- Métodos monetarios ----
    def parse_moneda(self, val):
        """Convierte cadenas con valores monetarios a float estandarizado."""
        if pd.isna(val):
            return 0
        s = str(val).strip()

        # 1) Detectar signo
        neg = ('(' in s and ')' in s) or any(ch in s for ch in self.MINUS_CHARS)

        # 2) Normalizar y quitar paréntesis/guiones/símbolos
        for ch in self.MINUS_CHARS:
            s = s.replace(ch, '')
        s = (s.replace('(', '')
               .replace(')', '')
               .replace('$', '')
               .replace('COP', '')
               .replace('€', '')
               .replace('£', '')
               .replace('US', '')
               .replace(' ', ''))

        # 3) Resolver separador decimal
        last_dot, last_comma = s.rfind('.'), s.rfind(',')
        if last_dot != -1 and last_comma != -1:
            dec = '.' if last_dot > last_comma else ','
            other = ',' if dec == '.' else '.'
            s = s.replace(other, '')
            s = s.replace(dec, '.')
        elif s.count(',') >= 1 and s.count('.') == 0:
            if s.count(',') == 1:
                s = s.replace(',', '.')
            else:
                s = s.replace(',', '')
        elif s.count('.') > 1 and s.count(',') == 0:
            s = s.replace('.', '')

        # 4) Dejar solo dígitos y punto
        s = re.sub(r'[^\d\.]', '', s)
        if s in ('', '.'):
            return 0

        try:
            n = Decimal(s)
        except InvalidOperation:
            return 0

        return float(-n if neg and n != 0 else n)

    # ---- Métodos para DataFrames ----
    def ordenar_columnas_mes(self, df, excluidas):
        """Ordena columnas de meses (MM-YYYY) dejando al inicio las excluidas."""
        columnas_mes = sorted(
            [col for col in df.columns if col not in excluidas],
            key=lambda x: pd.to_datetime(x, format='%m-%Y')
        )
        return df[excluidas + columnas_mes]

    def crear_tabla_resumen(self, df, index_cols, value_col, col_mes='MES_AÑO'):
        """
        Agrupa datos, pivotea tabla por meses y devuelve ordenada.
        index_cols: columnas para agrupar
        value_col: columna de valores a sumar
        col_mes: columna de periodo en formato MM-YYYY
        """
        tabla = df.groupby(index_cols + [col_mes])[value_col].sum().reset_index()
        tabla_pivot = tabla.pivot_table(
            index=index_cols,
            columns=col_mes,
            values=value_col,
            fill_value=0
        ).reset_index()
        return self.ordenar_columnas_mes(tabla_pivot, index_cols)