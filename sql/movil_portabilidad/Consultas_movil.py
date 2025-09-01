import pandas as pd
from datetime import datetime, timedelta


dia = (datetime.now() - timedelta(days=1)).day
año = datetime.now().year
año_anterior = datetime.now().year - 1

# Consulta: NEGOCIOS
query_negocios = f"""
SELECT [Base_Acumulada_2024].[SEGMENTO], [Base_Acumulada_2024].[DIA_VENTA], [Base_Acumulada_2024].[FECHAG],
       [Base_Acumulada_2024].[Año], [Base_Acumulada_2024].[Mes], [Base_Acumulada_2024].[TIPO_V],
       Count([Base_Acumulada_2024].[TELE_NUMB]) AS CuentaDeTELE_NUMB
FROM [Base_Acumulada_2024]
GROUP BY [Base_Acumulada_2024].[SEGMENTO], [Base_Acumulada_2024].[DIA_VENTA], [Base_Acumulada_2024].[FECHAG],
         [Base_Acumulada_2024].[Año], [Base_Acumulada_2024].[Mes], [Base_Acumulada_2024].[TIPO_V]
HAVING ((([Base_Acumulada_2024].[SEGMENTO])='NEGOCIOS')
        AND (([Base_Acumulada_2024].[DIA_VENTA])<={dia})
        AND (([Base_Acumulada_2024].[Año])={año_anterior})
        AND (([Base_Acumulada_2024].[TIPO_V]) LIKE '%port%'))

UNION

SELECT [Base_Acumulada].[SEGMENTO], [Base_Acumulada].[DIA_VENTA], [Base_Acumulada].[FECHAG],
       [Base_Acumulada].[Año], [Base_Acumulada].[Mes], [Base_Acumulada].[TIPO_V],
       Count([Base_Acumulada].[TELE_NUMB]) AS CuentaDeTELE_NUMB
FROM [Base_Acumulada]
GROUP BY [Base_Acumulada].[SEGMENTO], [Base_Acumulada].[DIA_VENTA], [Base_Acumulada].[FECHAG],
         [Base_Acumulada].[Año], [Base_Acumulada].[Mes], [Base_Acumulada].[TIPO_V]
HAVING ((([Base_Acumulada].[SEGMENTO])='NEGOCIOS')
        AND (([Base_Acumulada].[DIA_VENTA])<={dia})
        AND (([Base_Acumulada].[Año])={año})
        AND (([Base_Acumulada].[TIPO_V]) LIKE '%port%'));
"""

# Consulta: EMPRESAS
query_empresas = f"""
SELECT SEGMENTO, DIA_VENTA, FECHAG, Año, Mes, TIPO_V, Count(TELE_NUMB) AS CuentaDeTELE_NUMB
FROM Base_Acumulada_2024
GROUP BY SEGMENTO, DIA_VENTA, FECHAG, Año, Mes, TIPO_V
HAVING SEGMENTO='EMPRESAS' AND DIA_VENTA <= {dia} AND Año={año_anterior} AND TIPO_V LIKE '%port%'

UNION

SELECT SEGMENTO, DIA_VENTA, FECHAG, Año, Mes, TIPO_V, Count(TELE_NUMB) AS CuentaDeTELE_NUMB
FROM Base_Acumulada
GROUP BY SEGMENTO, DIA_VENTA, FECHAG, Año, Mes, TIPO_V
HAVING SEGMENTO='EMPRESAS' AND DIA_VENTA <= {dia} AND Año={año} AND TIPO_V LIKE '%port%'
"""



# Consulta: UMC
query_umc = f"""
SELECT DIA_VENTA, FECHAG, Año, Mes, TIPO_V, Count(TELE_NUMB) AS CuentaDeTELE_NUMB
FROM Base_Acumulada_2024
GROUP BY DIA_VENTA, FECHAG, Año, Mes, TIPO_V
HAVING DIA_VENTA <= {dia} AND Año={año_anterior} AND TIPO_V LIKE '%port%'

UNION

SELECT DIA_VENTA, FECHAG, Año, Mes, TIPO_V, Count(TELE_NUMB) AS CuentaDeTELE_NUMB
FROM Base_Acumulada
GROUP BY DIA_VENTA, FECHAG, Año, Mes, TIPO_V
HAVING DIA_VENTA <= {dia} AND Año={año} AND TIPO_V LIKE '%port%'
"""

# Consulta: MOVISTAR-TIGO
query_movistar_tigo = f"""
SELECT SEGMENTO, DIA_VENTA, FECHAG, Año, Mes, CONCESIONARIO, TIPO_V, Count(TELE_NUMB) AS CuentaDeTELE_NUMB
FROM Base_Acumulada_2024
GROUP BY SEGMENTO, DIA_VENTA, FECHAG, Año, Mes, CONCESIONARIO, TIPO_V
HAVING SEGMENTO='NEGOCIOS' AND DIA_VENTA <= {dia} AND Año={año_anterior} AND CONCESIONARIO IN ('MOVISTAR','TIGO') AND TIPO_V LIKE '%port%'

UNION

SELECT SEGMENTO, DIA_VENTA, FECHAG, Año, Mes, CONCESIONARIO, TIPO_V, Count(TELE_NUMB) AS CuentaDeTELE_NUMB
FROM Base_Acumulada
GROUP BY SEGMENTO, DIA_VENTA, FECHAG, Año, Mes, CONCESIONARIO, TIPO_V
HAVING SEGMENTO='NEGOCIOS' AND DIA_VENTA <= {dia} AND Año={año} AND CONCESIONARIO IN ('MOVISTAR','TIGO') AND TIPO_V LIKE '%port%'
"""