import logging
import pandas as pd

from integraciones.Proceso_Ingresos_Fo.procesamiento_ri import ProcesadorRI
from integraciones.Proceso_Ingresos_Fo.procesamiento_ciclos import ProcesadorCiclos
from integraciones.Proceso_Ingresos_Fo.Parametros import (
    ruta_ri,
    ruta_ciclos,
    ruta_salida,
    ruta_homologacion,
    dic_orden_columnas,
    dic_columnas_pesos,
)
from integraciones.Proceso_Ingresos_Fo.homologacion_reader import HomologacionReader

logger = logging.getLogger(__name__)


class RiDetalle:
    def __init__(self, df_bd_asignacion: pd.DataFrame):
        self.df_ri = ProcesadorRI(ruta_ri, ruta_salida).procesar()
        self.df_ciclos = ProcesadorCiclos(ruta_ciclos, ruta_salida).procesar()
        self.df_bd_asignacion = df_bd_asignacion
        self.df_homologacion = HomologacionReader(ruta_homologacion).leer_homologacion()

    # =======================
    # 1. CRUCE NIT
    # =======================
    def regla_cruce_nit(self) -> pd.DataFrame:
        """Cruza RI(IDOnyxFacturador) con CICLOS(Id cliente) para traer NIT."""
        logger.info(f"Columnas df_ri: {list(self.df_ri.columns)}")
        logger.info(f"Columnas df_ciclos: {list(self.df_ciclos.columns)}")
        logger.info("Ejemplos IDOnyxFacturador en RI:")
        logger.info(self.df_ri["IDOnyxFacturador"].dropna().unique()[:20])

        df_ciclos = self.df_ciclos.rename(columns={"Id cliente": "IDOnyxFacturador"})
        df_ciclos["IDOnyxFacturador"] = df_ciclos["IDOnyxFacturador"].astype(str).str.strip()
        self.df_ri["IDOnyxFacturador"] = self.df_ri["IDOnyxFacturador"].astype(str).str.strip()
        logger.info("Ejemplos Id cliente en Ciclos:")
        logger.info(df_ciclos)
        #df_ciclos.to_excel(r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\CICLOS_validacion.xlsx", index=False)
        #self.df_ri.to_excel(r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\RI_validacion.xlsx", index=False)

        df = self.df_ri.merge(df_ciclos, on="IDOnyxFacturador", how="left")
        #df.to_excel(r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\RI_CICLOS_merged.xlsx", index=False)
        logger.info(f"Cruce NIT aplicado: {len(df)} registros resultantes.")
        logger.info(df[["IDOnyxFacturador", "NIT_DV"]].head(20))

        
        return df

    # =======================
    # 2. CRUCE ASIGNACIÓN por NIT_DV
    # =======================
    def regla_cruce_asignacion(self, df_union: pd.DataFrame) -> pd.DataFrame:
        df_union["NIT_DV"] = df_union["NIT_DV"].astype(str)
        self.df_bd_asignacion["NIT"] = self.df_bd_asignacion["NIT"].astype(str)
        df_union["NIT"] = df_union["NIT_DV"].str.split("-").str[0]



        logger.info(f"Después de generar columna NIT en df_union: {len(df_union)} registros")#borrar ok
        logger.info(f"Registros en df_bd_asignacion original: {len(self.df_bd_asignacion)}")#borrar


        #  Asegurar que NIT sea único antes del primer merge
        self.df_bd_asignacion = self.df_bd_asignacion.drop_duplicates(subset=["NIT"], keep="first")
        logger.info(f"df_bd_asignacion tras drop_duplicates(NIT): {len(self.df_bd_asignacion)} registros")#borrar
    

        df_nit = df_union.merge(self.df_bd_asignacion, on="NIT", how="left")
        logger.info(f"Después de primer merge por NIT: {len(df_nit)} registros")


        df_nit["NIT_DV"] = df_nit["NIT_DV_x"].replace("", pd.NA).fillna(df_nit["NIT_DV_y"])
        df_nit = df_nit.drop(columns=["NIT_DV_x", "NIT_DV_y"])

        df_falta = df_nit[df_nit["RAZON_SOCIAL"].isna()]
        logger.info(f"Registros sin asignación después del primer merge: {len(df_falta)}")#borrar
        #df_nit = df_nit.drop(df_falta.index)
        if not df_falta.empty:
         logger.warning(f"{len(df_falta)} registros no encontraron asignación")
        df_nit.loc[df_nit["RAZON_SOCIAL"].isna(), "RAZON_SOCIAL"] = "SIN_ASIGNACION"

        df_por_dv = df_falta.merge(self.df_bd_asignacion.drop_duplicates(subset=["NIT_DV"]),
                                   on="NIT_DV", how="left")
        
        logger.info(f"Después de merge por NIT_DV usando df_falta: {len(df_por_dv)} registros")#borrar
        
        df_union_df = df_union[df_union['NIT_DV'].isin(df_falta['NIT_DV'])]

        logger.info(f"df_union_df (solo NIT_DV en falta): {len(df_union_df)} registros")


        #  Asegurar que NIT_DV sea único antes del segundo merge
        self.df_bd_asignacion = self.df_bd_asignacion.drop_duplicates(subset=["NIT_DV"], keep="first")
        logger.info(f"df_bd_asignacion tras drop_duplicates(NIT_DV): {len(self.df_bd_asignacion)} registros")

        df_por_dv = df_union_df.merge(self.df_bd_asignacion, on="NIT_DV", how="left")
        logger.info(f"Después de segundo merge por NIT_DV: {len(df_por_dv)} registros")#borrar

        df_por_dv["NIT"] = df_por_dv["NIT_x"].replace("", pd.NA).fillna(df_por_dv["NIT_y"])
        df_por_dv = df_por_dv.drop(columns=["NIT_x", "NIT_y"]) 

        #df_nit = pd.concat([df_nit, df_por_dv]).drop_duplicates().reset_index(drop=True)
        #df_nit.loc[df_nit["RAZON_SOCIAL"] == "SIN_ASIGNACION", :] = df_por_dv

        # Hacer merge solo sobre los que quedaron con SIN_ASIGNACION
        # Renombrar columnas en df_por_dv
        # Merge solo por los que faltan (por NIT_DV)
        # Renombrar columnas de df_por_dv para que no choquen con las de df_nit


        # Renombrar columnas de df_por_dv para que no choquen
        df_por_dv = df_por_dv.rename(columns={
            "RAZON_SOCIAL": "RAZON_SOCIAL_ajuste",
            "SEDE": "SEDE_ajuste",
            "SEGMENTO_ACTUAL": "SEGMENTO_ACTUAL_ajuste"
        })

        # 🔑 En vez de merge, hacemos diccionarios para mapear por NIT_DV
        map_razon = df_por_dv.set_index("NIT_DV")["RAZON_SOCIAL_ajuste"].to_dict()
        map_sede = df_por_dv.set_index("NIT_DV")["SEDE_ajuste"].to_dict()
        map_segmento = df_por_dv.set_index("NIT_DV")["SEGMENTO_ACTUAL_ajuste"].to_dict()

        # Reemplazar solo donde hay SIN_ASIGNACION
        df_nit.loc[df_nit["RAZON_SOCIAL"] == "SIN_ASIGNACION", "RAZON_SOCIAL"] = \
            df_nit["NIT_DV"].map(map_razon)

        df_nit.loc[df_nit["SEDE"] == "SIN_ASIGNACION", "SEDE"] = \
            df_nit["NIT_DV"].map(map_sede)

        df_nit.loc[df_nit["SEGMENTO_ACTUAL"] == "SIN_ASIGNACION", "SEGMENTO_ACTUAL"] = \
            df_nit["NIT_DV"].map(map_segmento)

        logger.info(f"Cruce asignacion aplicado por NIT_DV: {len(df_nit)} registros resultantes.")
        logger.info("Ejemplos de NIT_DV antes del cruce asignación:")
        logger.info(df_union['NIT_DV'].dropna().unique()[:20])

        return df_nit



    # =======================
    # 3. CRUCE Homologación por Servicio
    # =======================
    def regla_cruce_homologacion(self, df_nit: pd.DataFrame) -> pd.DataFrame:
        df_nit["Servicio"] = df_nit["Servicio"].astype(str).str.strip().str.upper()
        self.df_homologacion["Servicio"] = self.df_homologacion["Servicio"].astype(str).str.strip().str.upper()
        self.df_homologacion = self.df_homologacion.drop_duplicates(subset=["Servicio"], keep="first")

        servicios_nit = set(df_nit["Servicio"].unique())
        servicios_homologacion = set(self.df_homologacion["Servicio"].unique())

        diff_nit = list(servicios_nit - servicios_homologacion)[:20]
        diff_homologacion = list(servicios_homologacion - servicios_nit)[:20]

        if diff_nit:
            logger.warning(f"Ejemplos de servicios en df_nit que NO están en homologación: {diff_nit}")
        if diff_homologacion:
            logger.warning(f"Ejemplos de servicios en homologación que NO están en df_nit: {diff_homologacion}")

        df_homologizacion_cruce = df_nit.merge(self.df_homologacion, on="Servicio", how="left")
        logger.info(f"Cruce Homologacion aplicado: {len(df_homologizacion_cruce)} registros resultantes.")
        return df_homologizacion_cruce

    # =======================
    # 4. CRUCE planta comercial por Cédula Consultor
    # =======================
    def regla_cruce_planta(self, df_homologizacion_cruce: pd.DataFrame, df_bd_planta: pd.DataFrame) -> pd.DataFrame:
        df_homologizacion_cruce["CEDULA_CONSULTOR_FIJO"] = df_homologizacion_cruce["CEDULA_CONSULTOR_FIJO"].astype(str).str.strip()
        df_bd_planta["CEDULA_CONSULTOR_FIJO"] = df_bd_planta["CEDULA_CONSULTOR_FIJO"].astype(str).str.strip()

        antes = len(df_bd_planta)
        df_bd_planta = df_bd_planta.drop_duplicates(
            subset=["CEDULA_CONSULTOR_FIJO", "CONSULTOR_ACTUAL_FIJO"], keep="first"
        )
        logger.info(f"Duplicados eliminados en planta: {antes - len(df_bd_planta)}")

        df_planta = df_homologizacion_cruce.merge(
            df_bd_planta,
            on="CEDULA_CONSULTOR_FIJO",
            how="left",
            suffixes=("_hom", "_planta"),
        )

        for col in df_bd_planta.columns:
            if col != "CEDULA_CONSULTOR_FIJO" and col in df_homologizacion_cruce.columns:
                col_hom = f"{col}_hom"
                col_planta = f"{col}_planta"
                if col_hom in df_planta.columns and col_planta in df_planta.columns:
                    df_planta[col] = df_planta[col_hom].combine_first(df_planta[col_planta])

        df_planta = df_planta[
            [c for c in df_planta.columns if not (c.endswith("_hom") or c.endswith("_planta"))]
        ]

        logger.info(f"Cruce planta comercial aplicado con fusión: {len(df_planta)} registros resultantes.")
        return df_planta

    # =======================
    # EJECUTAR TODAS
    # =======================
    def ejecutar_reglas(self, df_bd_planta: pd.DataFrame) -> pd.DataFrame:
        df = self.regla_cruce_nit()
        df_2 = self.regla_cruce_asignacion(df)
        logger.info(f"Columnas después de cruce asignación: {list(df_2.columns)}")

        df_3 = self.regla_cruce_homologacion(df_2)
        logger.info(f"Columnas después de cruce homologación: {list(df_3.columns)}")

        df_4 = self.regla_cruce_planta(df_3, df_bd_planta)
        logger.info(f"Columnas después de cruce planta: {list(df_4.columns)}")

        orden_columnas = dic_orden_columnas["orden"]
        columnas_finales = [col for col in orden_columnas if col in df_4.columns]
        df_4 = df_4[columnas_finales]

        cols_a_pesos = dic_columnas_pesos["columnas_pesos"]
        for col in cols_a_pesos:
            if col in df_4.columns:
                # Convertir a número, NaN a 0
                df_4[col] = pd.to_numeric(df_4[col], errors="coerce").fillna(0).astype(int)


        df_4.columns = [col.title() for col in df_4.columns]

        df_4["Cedula_Consultor_Fijo"] = pd.to_numeric(
            df_4["Cedula_Consultor_Fijo"], errors="coerce"
        ).fillna(0).astype(int)

        df_4.columns = df_4.columns.str.upper()

        logger.info(f"Columnas disponibles antes de rename: {list(df_4.columns)}")
        logger.info(df_4[["NIT_DV", "NIT"]].head(10))


        df_4 = df_4.rename(columns={"NIT_DV": "NIT", "NIT": "NIT9"})

        return df_4

