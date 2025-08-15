import pandas as pd
import unicodedata
import re
from decimal import Decimal, InvalidOperation

def limpiar_texto(texto):
    if pd.isnull(texto):
        return ''
    texto = str(texto).strip().upper()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return texto

def clasificar_producto(producto):
    producto = producto.lower()
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

MINUS_CHARS = ['-', '–', '—', '−']  # hyphen, en dash, em dash, unicode minus

def parse_moneda(val):
    if pd.isna(val):
        return 0
    s = str(val).strip()

    # 1) Detectar signo
    neg = ('(' in s and ')' in s) or any(ch in s for ch in MINUS_CHARS)

    # 2) Normalizar y quitar paréntesis/guiones/símbolos
    for ch in MINUS_CHARS:
        s = s.replace(ch, '')
    s = (s.replace('(', '')
           .replace(')', '')
           .replace('$', '')
           .replace('COP', '')
           .replace('€', '')
           .replace('£', '')
           .replace('US', '')
           .replace(' ', ''))

    # 3) Resolver separador decimal:
    # si hay punto y coma, el decimal es el que aparece más a la derecha
    last_dot, last_comma = s.rfind('.'), s.rfind(',')
    if last_dot != -1 and last_comma != -1:
        dec = '.' if last_dot > last_comma else ','
        other = ',' if dec == '.' else '.'
        s = s.replace(other, '')
        s = s.replace(dec, '.')
    elif s.count(',') >= 1 and s.count('.') == 0:
        # solo comas: si hay una => decimal; si hay varias => miles
        if s.count(',') == 1:
            s = s.replace(',', '.')
        else:
            s = s.replace(',', '')
    elif s.count('.') > 1 and s.count(',') == 0:
        # solo puntos y varios => miles
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


def ordenar_columnas_mes(df, excluidas):
    columnas_mes = sorted(
        [col for col in df.columns if col not in excluidas],
        key=lambda x: pd.to_datetime(x, format='%m-%Y')
    )
    return df[excluidas + columnas_mes]


def crear_tabla_resumen(df, index_cols, value_col, col_mes='MES_AÑO'):
    tabla = df.groupby(index_cols + [col_mes])[value_col].sum().reset_index()
    tabla_pivot = tabla.pivot_table(index=index_cols, columns=col_mes, values=value_col, fill_value=0).reset_index()
    return ordenar_columnas_mes(tabla_pivot, index_cols)