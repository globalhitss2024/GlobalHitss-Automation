#---------------------------------------------------PARAMETROS GENERALES  -------------------------------------------------

#CONEXION BD
usuario = 'postgres'
contrasena = '1Nt3l163nC14_C0m3rc14L'
host = 'localhost'
port = '5432'
bd_inteligencia_comercial = 'DBInteligenciaComercial'

#CONFIGURACION DE LOG (SOLO APLICA PARA WINDOWS)
ruta_log = "C:/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_produccion/Log"

#Conflguracion de Log (Solo aplica para windows)
ruta_log = "C:/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_produccion/Log"

#---------------------------------------------------PARAMETROS VALOR AGREGADO-------------------------------------------------
ruta_fuente_valor_agregado= 'C:/Users/INTCOM/OneDrive - Comunicacion Celular S.A.- Comcel S.A/Jefatura Control de Información - 04_EYN_MOVIL/'
nombre_archivo_valor_agregado = 'REPORTE VENTAS VALOR AGREGADO GENERAL_.xlsx'
nombre_hoja_valor_agregado = '2025'
destino_valor_agregado = 'tb_datos_crudos_valor_agregado'
 
#---------------------------------------------------PARAMETROS LEGALIZADAS -------------------------------------------------

#PARAMETROS LEGALIZADAS
#ruta_fuente_legalizadas='C:/ambiente_desarrollo/dev-empresas-negocios-env/fuentes/base_legalizadas/'
ruta_fuente_legalizadas='C:/01_Inteligencia_Comercial/Negocios Fijo/'
nombre_archivo_legalizadas1='Legalizadas.xlsb'
nombre_hoja_legalizadas1='PPPPPPP_T'
nombre_archivo_legalizadas2='Legalizadas_Correos.xlsx'
nombre_hoja_legalizadas2='Legalizadas_Correos'
nombre_archivo_legalizadas3 = 'Ubicacion_Contrato.csv'


#PARAMETROS MONITOREO
fuente_legalizadas = f'{nombre_archivo_legalizadas1} | {nombre_hoja_legalizadas2} | {nombre_archivo_legalizadas3}'
destino_legalizadas = 'tb_datos_crudos_legalizadas'



#COLUMNAS LEGALIZADAS
mapeo_columnas = {
    'id': 'id_legalizadas',
    'id_ejecucion': 'id_ejecucion_legalizadas',
    'cuenta': 'cuenta_legalizadas',
    'ot': 'ot',
    'fecha_legalizada': 'fecha_legalizada_legalizadas',
    'fecha_procesamiento': 'fecha_procesamiento_legalizadas',
    'fuente': 'fuente_legalizadas',
    'id_estado': 'id_estado_legalizadas'
}


#------------------------------------------------PARAMETROS PLANTA COMERCIAL  -------------------------------------------------

#PARAMETROS PLANTA COMERCIAL
ruta_fuente_planta_comercial = 'C:/Users/INTCOM/OneDrive - Comunicacion Celular S.A.- Comcel S.A/Planta Comercial/'
ruta_fuente_planta_comercial_ajuste = 'C:/ambiente_desarrollo/dev-empresas-negocios-env/fuentes/base_planta_comercial/'
nombre_archivo_planta = 'PLANTA COMERCIAL NUEVA ESTRUCTURA.xlsx'
nombre_hoja_red_maestra = 'RED MAESTRA'
nombre_hoja_red_retiro = 'RETIRO'
nombre_hoja_red_retail = 'RETAIL'
nombre_hoja_red_directos = 'DIRECTOS'
nombre_hoja_red_cavs = 'CAV´S'
nombre_hoja_red_tmk = 'TMK'

#PARAMETROS CLASIFICADOR GEOGRAFICO

ruta_fuente_clasificador_geografico= 'D:/DIEGO/Empresas y Negocios - CLARO/ambiente_desarrollo/dev-empresas-negocios-env/fuentes/base_planta_comercial/'
nombre_archivo_clasificador_geo = 'Clasificador Geográfico_v2.xlsx'
nombre_hoja_clasificador = 'clasificador'

#---------------------------------------------------PARAMETROS BASE CLOUD -------------------------------------------------
#PARAMETROS DE CONFIGURACION

#PARAMETROS CLOUD
ruta_fuente_cloud = 'C:/ambiente_desarrollo\dev-empresas-negocios-env/fuentes/base_cloud/'
nombre_archivo_cloud ='Reporte de Ventas Cloud mes en curso.xlsx'
nombre_hoja_cloud ='Hoja1'

#REGLAS DE NEGOCIO BASE CLOUD
# variable constante - explicacion

#PARAMETROS MONITOREO
fuente_cloud = f'{nombre_archivo_cloud}'
destino_cloud = 'tb_datos_crudos_cloud'

#Columnas cloud
mapeo_columnas_cloud = {
    'id':'id_cloud',
    'id_ejecucion':'id_ejecucion_cloud',
    'llave': 'llave_cloud',
    'fecha_de_venta': 'fecha_de_venta_cloud',
    'mes': 'mes_cloud',
    'ano': 'ano_cloud',
    'red': 'red_cloud',
    'tipo_red':'tipo_red_cloud',
    'id_cliente': 'id_cliente_cloud',
    'razon_social': 'razon_social_cloud',
    'nit': 'nit_cloud',
    'tipo_de_venta': 'tipo_de_venta_cloud',
    'segmento': 'segmento_cloud',
    'segmento_general': 'segmento_general_cloud',
    'asignacion_crm': 'asignacion_crm_cloud',
    'customer_id': 'customer_id_cloud',
    'idsuscription': 'idsuscription_cloud',
    'ot': 'ot',
    'sku_parallels': 'sku_parallels_cloud',
    'tipo_servicio': 'tipo_servicio_cloud',
    'producto': 'producto_cloud',
    'descripcion_plan': 'descripcion_plan_cloud',
    'no_servicios': 'no_servicios_cloud',
    'valor_unitario_sin_iva': 'valor_unitario_sin_iva_cloud',
    'valor_unitario_iva_incluido': 'valor_unitario_iva_incluido_cloud',
    'valor_total_sin_iva': 'valor_total_sin_iva_cloud',
    'valor_total_iva_incluido': 'valor_total_iva_incluido_cloud',
    'cedula_comercial': 'cedula_comercial_cloud',
    'nombres': 'nombres_cloud',
    'gerencia': 'gerencia_cloud',
    'jefatura': 'jefatura_cloud',
    'cordinacion': 'cordinacion_cloud',
    'cargo': 'cargo_cloud',
    'contratacion': 'contratacion_cloud',
    'proveedor': 'proveedor_cloud',
    'ciudad': 'ciudad_cloud',
    'estado_comercial_inicial_del_servicio_vendido': 'estado_comercial_inicial_del_servicio_vendido_cloud',
    'estado_comercial_final_del_servicio_vendido': 'estado_comercial_final_del_servicio_vendido_cloud',
    'observaciones_comercial_nombre_dominio': 'observaciones_comercial_nombre_dominio_cloud',
    'promociones': 'promociones_cloud',
    'fecha_activacion_parallels_del_servicio': 'fecha_activacion_parallels_del_servicio_cloud',
    'mes_activacion_parallels_del_servicio': 'mes_activacion_parallels_del_servicio_cloud',
    'ano_activacion_parallels': 'ano_activacion_parallels_cloud',
    'nombre_ingeniero_activacion_pa': 'nombre_ingeniero_activacion_pa_cloud',
    'fecha_legalizacion_ventas': 'fecha_legalizacion_ventas_cloud',
    'mes_legalizacion_ventas': 'mes_legalizacion_ventas_cloud',
    'ano_legalizacion': 'ano_legalizacion_cloud',
    'cod_incidente_onyx': 'cod_incidente_onyx_cloud',
    'cod_ot_generada': 'cod_ot_generada_cloud',
    'observaciones': 'observaciones_cloud',
    'valor_total_sin_iva_movimientos_recurrentes_up_grade': 'valor_total_sin_iva_movimientos_recurrentes_up_grade_cloud',
    'valor_unitario_sin_iva2': 'valor_unitario_sin_iva2_cloud',
    'valor_unitario_iva_incluido3': 'valor_unitario_iva_incluido3_cloud',
    'valor_total_sin_iva2': 'valor_total_sin_iva2_cloud',
    'valor_total_iva_incluido5': 'valor_total_iva_incluido5_cloud',
    'tipo_de_activacion': 'tipo_de_activacion_cloud',
    'valor_en_el_sistema_odin': 'valor_en_el_sistema_odin_cloud',
    'tipo_de_renta_one_time_anual_mensual': 'tipo_de_renta_one_time_anual_mensual_cloud',
    'valor_total_sin_iva_normalizado_valor_odin_12_todos_los_produ': 'valor_total_sin_iva_normalizado_valor_odin_12_todos_los_produ_cloud',
    'segmento_de_alta': 'segmento_de_alta_cloud',
    'director': 'director_cloud',
    'nombre_de_base': 'nombre_de_base_cloud',
    'reto_estrategico': 'reto_estrategico_cloud',
    'fecha_procesamiento': 'fecha_procesamiento_cloud',
	'fuente': 'fuente_cloud',
	'id_estado':'id_estado_cloud'
}

#---------------------------------------------------PARAMETROS BASE DIGITADAS -------------------------------------------------

#PARAMETROS DIGITADAS
ruta_fuente_digitadas = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_digitadas ='DIGITADAS.XLS'
nombre_hoja_digitadas ='DIGITADAS'

#PARAMETROS MONITOREO
fuente_digitadas = f'{nombre_archivo_digitadas}'
destino_digitadas = 'tb_datos_crudos_digitadas'

mapeo_columnas_digitadas = {
    'id':'id_digitadas',
    'id_ejecucion':'id_ejecucion_digitadas',
    'cuenta':'cuenta_digitadas',
    'ot':'ot',
    'numero_contrato': 'numero_contrato_digitadas',
    'nombres':'nombres_digitadas',
    'tipo_documento': 'tipo_documento_digitadas',
    'numero_documento': 'numero_documento_digitadas',
    'numero_telefono_1':'numero_telefono_1_digitadas',
    'numero_telefono_2':'numero_telefono_2_digitadas',
    'calle': 'calle_digitadas',
    'direccion_residencia': 'direccion_residencia_digitadas',
    'numero_apartamento': 'numero_apartamento_digitadas',
    'ciudad_venta': 'ciudad_venta_digitadas',
    'codigo_division': 'codigo_division_digitadas',
    'tipo_suscriptor': 'tipo_suscriptor_digitadas',
    'estado': 'estado_digitadas',
    'tarifa': 'tarifa_digitadas',
    'campana_1': 'campana_1_digitadas',
    'campana_2': 'campana_2_digitadas',
    'campana_3': 'campana_3_digitadas',
    'codigo_servicio': 'codigo_servicio_digitadas',
    'nombre_servicio': 'nombre_servicio_digitadas',
    'descripcion_servicio': 'descripcion_servicio_digitadas',
    'numero_dealer': 'numero_dealer_digitadas',
    'nombre_dealer': 'nombre_dealer_digitadas',
    'grupo_dealer': 'grupo_dealer_digitadas',
    'coordinador': 'coordinador_digitadas',
    'tipo_servicio': 'tipo_servicio_digitadas',
    'wo_kind': 'wo_kind_digitadas',
    'razon_servicio': 'razon_servicio_digitadas',
    'nodo': 'nodo_digitadas',
    'nombre_nodo': 'nombre_nodo_digitadas',
    'wo_creador': 'wo_creador_digitadas',
    'fecha_creacion': 'fecha_creacion_digitadas',
    'tipo_venta': 'tipo_venta_digitadas',
    'val_dif_service': 'val_dif_service_digitadas',
    'valor_servicio': 'valor_servicio_digitadas',
    'renta_wo_anterior': 'renta_wo_anterior_digitadas',
    'renta_wo_actual': 'renta_wo_actual_digitadas',
    'diferencia_renta': 'diferencia_renta_digitadas',
    'hora_creacion': 'hora_creacion_digitadas',
    'numero_lineas_suscriptor': 'numero_lineas_suscriptor_digitadas',
    'numero_servicios': 'numero_servicios_digitadas',
    'origen_datos': 'origen_datos_digitadas',
    'estrato': 'estrato_digitadas',
    'numeral_2': 'numeral_2_digitadas',
    'conyugue': 'conyugue_digitadas',
    'cod_black_list': 'cod_black_list_digitadas',
    'desc_black_list': 'desc_black_list_digitadas',
    'email': 'email_digitadas',
    'adicional_inf1': 'adicional_inf1_digitadas',
    'fecha_permanencia': 'fecha_permanencia_digitadas',
    'segmento': 'segmento_digitadas',
    'especialista': 'especialista_digitadas',
    'area_gcia_vtas': 'area_gcia_vtas_digitadas',
    'zona_gcia_vtas': 'zona_gcia_vtas_digitadas',
    'canal': 'canal_digitadas',
    'aliado': 'aliado_digitadas',
    'poblacion': 'poblacion_digitadas',
    'area_venta': 'area_venta_digitadas',
    'zona_venta': 'zona_venta_digitadas',
    'distrito': 'distrito_digitadas',
    'tipo_red': 'tipo_red_digitadas',
    'grupo_servicio':'grupo_servicio_digitadas',
    'fecha_procesamiento': 'fecha_procesamiento_digitadas',
	'fuente': 'fuente_digitadas',
	'id_estado':'id_estado_digitadas'
}

#---------------------------------------------------PARAMETROS BASE DIGITADAS UP -------------------------------------------------

#PARAMETROS DIGITADAS UP
ruta_fuente_digitadas_up = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_digitadas_up ='DIGITADASUP.csv'

#PARAMETROS MONITOREO
fuente_digitadas_up = f'{nombre_archivo_digitadas_up}'
destino_digitadas_up = 'tb_datos_crudos_digitadas_up'

#Columnas digitadas up
mapeo_columnas_digitadas_up = {
    'id':'id_digitadas_up',
    'id_ejecucion':'id_ejecucion_digitadas_up',
    'cuenta':'cuenta_digitadas_up',
    'ot':'ot',
    'service_code':'service_code_digitadas_up',
    'monthly_rental_charge':'monthly_rental_charge_digitadas_up',
    'renta_mes_anterior':'renta_mes_anterior_digitadas_up',
    'cod_servicio_anterior':'cod_servicio_anterior_digitadas_up',
    'tipo_cambio':'tipo_cambio_digitadas_up',
    'fecha_procesamiento': 'fecha_procesamiento_digitadas_up',
	'fuente': 'fuente_digitadas_up',
	'id_estado':'id_estado_digitadas_up'
}

#---------------------------------------------------PARAMETROS BASE INSTALADAS UP -------------------------------------------------

#PARAMETROS INSTALADAS UP
ruta_fuente_instaladas_up = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_instaladas_up ='INSTALADASUP.csv'

#PARAMETROS MONITOREO
fuente_instaladas_up = f'{nombre_archivo_instaladas_up}'
destino_instaladas_up = 'tb_datos_crudos_instaladas_up'

#Columnas instaladas up
mapeo_columnas_instaladas_up = {
    'id':'id_instaladas_up',
    'id_ejecucion':'id_ejecucion_instaladas_up',
    'cuenta':'cuenta_instaladas_up',
    'ot':'ot',
    'service_code':'service_code_instaladas_up',
    'monthly_rental_charge':'monthly_rental_charge_instaladas_up',
    'renta_mes_anterior':'renta_mes_anterior_instaladas_up',
    'cod_servicio_anterior':'cod_servicio_anterior_instaladas_up',
    'tipo_cambio':'tipo_cambio_instaladas_up',
    'fecha_procesamiento': 'fecha_procesamiento_instaladas_up',
	'fuente': 'fuente_instaladas_up',
	'id_estado':'id_estado_instaladas_up'
}

#---------------------------------------------------PARAMETROS BASE MAESTRO CÓDIGOS -------------------------------------------------

#PARAMETROS MAESTRO DE CODIGOS
ruta_fuente_maestro_codigos = 'C:/Users/INTCOM\OneDrive - Comunicacion Celular S.A.- Comcel S.A/Maestro Códigos de Servicio HFC-DTH-FTTH/'
nombre_archivo_maestro_codigos ='Maestro Códigos Servicio HFC Valores.xlsb'
nombre_hoja_maestro_codigos ='SRVMSTR'

#PARAMETROS MONITOREO
fuente_maestro_codigos = f'{nombre_archivo_maestro_codigos}'
destino_maestro_codigos = 'tb_datos_crudos_maestro_codigos'

#Columnas maestro de codigos
mapeo_columnas_maestro_codigos = {
    'id': 'id_maestro_codigos',
    'id_ejecucion': 'id_ejecucion_maestro_codigos',
    'saserv': 'saserv', 
    'saname': 'saname_maestro_codigos', 
    'sacatg': 'sacatg_maestro_codigos',
    'sajuri': 'sajuri_maestro_codigos',
    'sasgrp': 'sasgrp_maestro_codigos',
    'clasificacion': 'clasificacion_maestro_codigos',
    'clas_planeacion': 'clas_planeacion_maestro_codigos',
    'servicios': 'servicios_maestro_codigos',
    'velocidad_kbps': 'velocidad_kbps_maestro_codigos',
    'p_g_gestion': 'p_g_gestion_maestro_codigos',
    'resumen_comentarios': 'resumen_comentarios_maestro_codigos',
    'producto_homologado': 'producto_homologado_maestro_codigos',
    'cebe_sinergia': 'cebe_sinergia_maestro_codigos',
    'cebe_fusion': 'cebe_fusion_maestro_codigos',
    'observaciones': 'observaciones_maestro_codigos',
    'tecnologia_principal': 'tecnologia_principal_maestro_codigos',
    'plataforma': 'plataforma_maestro_codigos',
    'tv_hd': 'tv_hd_maestro_codigos',
    'capacidad_navegacion': 'capacidad_navegacion_maestro_codigos',
    'cargar_servicio':'cargar_servicio_maestro_codigos',
    'fecha_procesamiento': 'fecha_procesamiento_maestro_codigos',
	'fuente': 'fuente_maestro_codigos',
	'id_estado':'id_estado_maestro_codigos'
}

#---------------------------------------------------PARAMETROS BASE VENTAS COMPRAS -------------------------------------------------

#PARAMETROS VENTAS COMPRAS
ruta_fuente_ventas_compras = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_ventas_compras ='VENTASCOMPRAS.csv'

#PARAMETROS MONITOREO
fuente_ventas_compras = f'{nombre_archivo_ventas_compras}'
destino_ventas_compras = 'tb_datos_crudos_ventas_compras'

mapeo_columnas_ventas_compras = {
    'id':'id_ventas_compras',
    'id_ejecucion':'id_ejecucion_ventas_compras',
    'ot':'ot',
    'codigo_servicio_ad':'codigo_servicio_ad_ventas_compras',
    'cuenta':'cuenta_ventas_compras',
    'fecha_estado':'fecha_estado_ventas_compras',
    'cantidad':'cantidad_ventas_compras',
    'plazo':'plazo_ventas_compras',
    'valor_total':'valor_total_ventas_compras',
    'valor':'valor_ventas_compras',
    'red':'red_ventas_compras',
    'fecha_procesamiento': 'fecha_procesamiento_ventas_compras',
	'fuente': 'fuente_ventas_compras',
	'id_estado':'id_estado_ventas_compras'
}

#---------------------------------------------------PARAMETROS BASE INSTALADAS -------------------------------------------------
#PARAMETROS INSTALADAS
ruta_fuente_instaladas = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_instaladas ='INSTALADAS.xls'
nombre_hoja_instaladas ='INSTALADAS'

#PARAMETROS MONITOREO
fuente_instaladas = f'{nombre_archivo_instaladas}'
destino_instaladas = 'tb_datos_crudos_instaladas'

mapeo_columnas_instaladas = {
    'id':'id_instaladas',
    'id_ejecucion':'id_ejecucion_instaladas',
    'cuenta':'cuenta_instaladas',
    'ot':'ot',
    'numero_contrato': 'numero_contrato_instaladas',
    'nombres':'nombres_instaladas',
    'tipo_documento': 'tipo_documento_instaladas',
    'numero_documento': 'numero_documento_instaladas',
    'numero_telefono_1':'numero_telefono_1_instaladas',
    'numero_telefono_2':'numero_telefono_2_instaladas',
    'numero_calle': 'numero_calle_instaladas',
    'direccion_residencia': 'direccion_residencia_instaladas',
    'numero_apartamento': 'numero_apartamento_instaladas',
    'ciudad_venta': 'ciudad_venta_instaladas',
    'codigo_division': 'codigo_division_instaladas',
    'tipo_suscriptor': 'tipo_suscriptor_instaladas',
    'estado': 'estado_instaladas',
    'tarifa': 'tarifa_instaladas',
    'campana_1': 'campana_1_instaladas',
    'campana_2': 'campana_2_instaladas',
    'campana_3': 'campana_3_instaladas',
    'codigo_servicio': 'codigo_servicio_instaladas',
    'nombre_servicio': 'nombre_servicio_instaladas',
    'descripcion_servicio': 'descripcion_servicio_instaladas',
    'numero_dealer': 'numero_dealer_instaladas',
    'nombre_dealer': 'nombre_dealer_instaladas',
    'grupo_dealer': 'grupo_dealer_instaladas',
    'coordinador': 'coordinador_instaladas',
    'tipo_servicio': 'tipo_servicio_instaladas',
    'wo_kind': 'wo_kind_instaladas',
    'razon_servicio': 'razon_servicio_instaladas',
    'nodo': 'nodo_instaladas',
    'nombre_nodo': 'nombre_nodo_instaladas',
    'wo_creador': 'wo_creador_instaladas',
    'fecha_digitacion': 'fecha_digitacion_instaladas',
    'fecha_completado': 'fecha_completado_instaladas',
    'tipo_venta': 'tipo_venta_instaladas',
    'val_dif_service': 'val_dif_service_instaladas',
    'valor_servicio': 'valor_servicio_instaladas',
    'renta_wo_anterior': 'renta_wo_anterior_instaladas',
    'renta_wo_actual': 'renta_wo_actual_instaladas',
    'diferencia_renta': 'diferencia_renta_instaladas',
    'numero_lineas_suscriptor': 'numero_lineas_suscriptor_instaladas',
    'numero_servicios': 'numero_servicios_instaladas',
    'origen_datos': 'origen_datos_instaladas',
    'estrato': 'estrato_instaladas',
    'numeral_2': 'numeral_2_instaladas',
    'conyugue': 'conyugue_instaladas',
    'cod_black_list': 'cod_black_list_instaladas',
    'desc_black_list': 'desc_black_list_instaladas',
    'email': 'email_instaladas',
    'adicional_inf1': 'adicional_inf1_instaladas',
    'fecha_permanencia': 'fecha_permanencia_instaladas',
    'especialista': 'especialista_instaladas',
    'area_gcia_vtas': 'area_gcia_vtas_instaladas',
    'zona_gcia_vtas': 'zona_gcia_vtas_instaladas',
    'canal': 'canal_instaladas',
    'aliado': 'aliado_instaladas',
    'poblacion': 'poblacion_instaladas',
    'area_venta': 'area_venta_instaladas',
    'zona_venta': 'zona_venta_instaladas',
    'distrito': 'distrito_instaladas',
    'tipo_red': 'tipo_red_instaladas',
    'grupo_servicio':'grupo_servicio_instaladas',
    'fecha_procesamiento': 'fecha_procesamiento_instaladas',
	'fuente': 'fuente_instaladas',
	'id_estado':'id_estado_instaladas'
}

#---------------------------------------------------PARAMETROS BASE INSTALADAS 999 -------------------------------------------------
#PARAMETROS INSTALADAS 999
ruta_fuente_instaladas_999 = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_instaladas_999 ='INSTALADAS 999.xls'
nombre_hoja_instaladas_999 ='INSTALADAS 999'

#PARAMETROS MONITOREO
fuente_instaladas_999 = f'{nombre_archivo_instaladas_999}'
destino_instaladas_999 = 'tb_datos_crudos_instaladas_999'

mapeo_columnas_instaladas_999 = {
    'id':'id_instaladas_999',
    'id_ejecucion':'id_ejecucion_instaladas_999',
    'cuenta':'cuenta_instaladas_999',
    'ot':'ot',
    'numero_contrato': 'numero_contrato_instaladas_999',
    'nombres':'nombres_instaladas_999',
    'tipo_documento': 'tipo_documento_instaladas_999',
    'numero_documento': 'numero_documento_instaladas_999',
    'numero_telefono_1':'numero_telefono_1_instaladas_999',
    'numero_telefono_2':'numero_telefono_2_instaladas_999',
    'numero_calle': 'numero_calle_instaladas_999',
    'direccion_residencia': 'direccion_residencia_instaladas_999',
    'numero_apartamento': 'numero_apartamento_instaladas_999',
    'ciudad_venta': 'ciudad_venta_instaladas_999',
    'codigo_division': 'codigo_division_instaladas_999',
    'tipo_suscriptor': 'tipo_suscriptor_instaladas_999',
    'estado': 'estado_instaladas_999',
    'tarifa': 'tarifa_instaladas_999',
    'campana_1': 'campana_1_instaladas_999',
    'campana_2': 'campana_2_instaladas_999',
    'campana_3': 'campana_3_instaladas_999',
    'codigo_servicio': 'codigo_servicio_instaladas_999',
    'nombre_servicio': 'nombre_servicio_instaladas_999',
    'descripcion_servicio': 'descripcion_servicio_instaladas_999',
    'numero_dealer': 'numero_dealer_instaladas_999',
    'nombre_dealer': 'nombre_dealer_instaladas_999',
    'grupo_dealer': 'grupo_dealer_instaladas_999',
    'coordinador': 'coordinador_instaladas_999',
    'tipo_servicio': 'tipo_servicio_instaladas_999',
    'wo_kind': 'wo_kind_instaladas_999',
    'razon_servicio': 'razon_servicio_instaladas_999',
    'nodo': 'nodo_instaladas_999',
    'nombre_nodo': 'nombre_nodo_instaladas_999',
    'wo_creador': 'wo_creador_instaladas_999',
    'fecha_digitacion': 'fecha_digitacion_instaladas_999',
    'fecha_completado': 'fecha_completado_instaladas_999',
    'tipo_venta': 'tipo_venta_instaladas_999',
    'val_dif_service': 'val_dif_service_instaladas_999',
    'valor_servicio': 'valor_servicio_instaladas_999',
    'renta_wo_anterior': 'renta_wo_anterior_instaladas_999',
    'renta_wo_actual': 'renta_wo_actual_instaladas_999',
    'diferencia_renta': 'diferencia_renta_instaladas_999',
    'numero_lineas_suscriptor': 'numero_lineas_suscriptor_instaladas_999',
    'numero_servicios': 'numero_servicios_instaladas_999',
    'origen_datos': 'origen_datos_instaladas_999',
    'estrato': 'estrato_instaladas_999',
    'numeral_2': 'numeral_2_instaladas_999',
    'conyugue': 'conyugue_instaladas_999',
    'cod_black_list': 'cod_black_list_instaladas_999',
    'desc_black_list': 'desc_black_list_instaladas_999',
    'email': 'email_instaladas_999',
    'adicional_inf1': 'adicional_inf1_instaladas_999',
    'fecha_permanencia': 'fecha_permanencia_instaladas_999',
    'especialista': 'especialista_instaladas_999',
    'area_gcia_vtas': 'area_gcia_vtas_instaladas_999',
    'zona_gcia_vtas': 'zona_gcia_vtas_instaladas_999',
    'canal': 'canal_instaladas_999',
    'aliado': 'aliado_instaladas_999',
    'poblacion': 'poblacion_instaladas_999',
    'area_venta': 'area_venta_instaladas_999',
    'zona_venta': 'zona_venta_instaladas_999',
    'distrito': 'distrito_instaladas_999',
    'tipo_red': 'tipo_red_instaladas_999',
    'grupo_venta':'grupo_venta_instaladas_999',
    'fecha_procesamiento': 'fecha_procesamiento_instaladas_999',
	'fuente': 'fuente_instaladas_999',
	'id_estado':'id_estado_instaladas_999'
}

#---------------------------------------------------PARAMETROS BASE CANCELADAS -------------------------------------------------

#PARAMETROS CANCELADAS
ruta_fuente_canceladas = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_canceladas ='CANCELADAS.xls'
nombre_hoja_canceladas ='CANCELADAS'

#PARAMETROS MONITOREO
fuente_canceladas = f'{nombre_archivo_canceladas}'
destino_canceladas = 'tb_datos_crudos_canceladas'

mapeo_columnas_canceladas = {
    'id':'id_canceladas',
    'id_ejecucion':'id_ejecucion_canceladas',
    'cuenta':'cuenta_canceladas',
    'ot':'ot',
    'nombres':'nombres_canceladas',
    'numero_telefono_1':'numero_telefono_1_canceladas',
    'numero_telefono_2':'numero_telefono_2_canceladas',
    'calle': 'calle_canceladas',
    'direccion': 'direccion_canceladas',
    'numero_apartamento': 'numero_apartamento_canceladas',
    'ciudad_venta': 'ciudad_venta_canceladas',
    'codigo_division': 'codigo_division_canceladas',
    'tipo_suscriptor': 'tipo_suscriptor_canceladas',
    'estado': 'estado_canceladas',
    'codigo_servicio': 'codigo_servicio_canceladas',
    'nombre_servicio': 'nombre_servicio_canceladas',
    'descripcion_servicio': 'descripcion_servicio_canceladas',
    'valor_servicio': 'valor_servicio_canceladas',
    'numero_asesor': 'numero_asesor_canceladas',
    'nombre_asesor': 'nombre_asesor_canceladas',
    'tipo_orden': 'tipo_orden_canceladas',
    'usuario_creador': 'usuario_creador_canceladas',
    'fecha_creacion': 'fecha_creacion_canceladas',
    'hora_creacion': 'hora_creacion_canceladas',
    'fecha_cancelada': 'fecha_cancelada_canceladas',
    'hora_cancelada': 'hora_cancelada_canceladas',
    'migracion': 'migracion_canceladas',
    'codigo_cancelacion': 'codigo_cancelacion_canceladas',
    'descripcion_cancelacion': 'descripcion_cancelacion_canceladas',
    'estrato': 'estrato_canceladas',
    'numeral_2': 'numeral_2_canceladas',
    'conyugue': 'conyugue_canceladas',
    'cod_black_list': 'cod_black_list_canceladas',
    'desc_black_list': 'desc_black_list_canceladas',
    'email': 'email_canceladas',
    'inf_adici_01': 'inf_adici_01_canceladas',
    'fecha_permanencia': 'fecha_permanencia_canceladas',
    'distrio': 'distrio_canceladas',
    'tipo_red': 'tipo_red_canceladas',
    'grupo_servicio':'grupo_servicio_canceladas',
    'fecha_procesamiento': 'fecha_procesamiento_canceladas',
	'fuente': 'fuente_canceladas',
	'id_estado':'id_estado_canceladas'
}

#---------------------------------------------------PARAMETROS BASE CANCELADAS 999 -------------------------------------------------

#PARAMETROS CANCELADAS 999
ruta_fuente_canceladas_999 = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_canceladas_999 ='CANCELADAS 999.xls'
nombre_hoja_canceladas_999 ='CANCELADAS 999'

#PARAMETROS MONITOREO
fuente_canceladas_999 = f'{nombre_archivo_canceladas_999}'
destino_canceladas_999 = 'tb_datos_crudos_canceladas_999'

mapeo_columnas_canceladas_999 = {
    'id':'id_canceladas_999',
    'id_ejecucion':'id_ejecucion_canceladas_999',
    'cuenta':'cuenta_canceladas_999',
    'ot':'ot',
    'nombres':'nombres_canceladas_999',
    'numero_telefono_1':'numero_telefono_1_canceladas_999',
    'numero_telefono_2':'numero_telefono_2_canceladas_999',
    'calle': 'calle_canceladas_999',
    'direccion': 'direccion_canceladas_999',
    'numero_apartamento': 'numero_apartamento_canceladas_999',
    'ciudad_venta': 'ciudad_venta_canceladas_999',
    'codigo_division': 'codigo_division_canceladas_999',
    'tipo_suscriptor': 'tipo_suscriptor_canceladas_999',
    'estado': 'estado_canceladas_999',
    'codigo_servicio': 'codigo_servicio_canceladas_999',
    'nombre_servicio': 'nombre_servicio_canceladas_999',
    'descripcion_servicio': 'descripcion_servicio_canceladas_999',
    'valor_servicio': 'valor_servicio_canceladas_999',
    'numero_asesor': 'numero_asesor_canceladas_999',
    'nombre_asesor': 'nombre_asesor_canceladas_999',
    'tipo_orden': 'tipo_orden_canceladas_999',
    'usuario_creador': 'usuario_creador_canceladas_999',
    'fecha_creacion': 'fecha_creacion_canceladas_999',
    'hora_creacion': 'hora_creacion_canceladas_999',
    'fecha_cancelada': 'fecha_cancelada_canceladas_999',
    'hora_cancelada': 'hora_cancelada_canceladas_999',
    'migracion': 'migracion_canceladas_999',
    'codigo_cancelacion': 'codigo_cancelacion_canceladas_999',
    'descripcion_cancelacion': 'descripcion_cancelacion_canceladas_999',
    'estrato': 'estrato_canceladas_999',
    'numeral_2': 'numeral_2_canceladas_999',
    'conyugue': 'conyugue_canceladas_999',
    'cod_black_list': 'cod_black_list_canceladas_999',
    'desc_black_list': 'desc_black_list_canceladas_999',
    'email': 'email_canceladas_999',
    'inf_adici_01': 'inf_adici_01_canceladas_999',
    'fecha_permanencia': 'fecha_permanencia_canceladas_999',
    'fecha_procesamiento': 'fecha_procesamiento_canceladas_999',
	'fuente': 'fuente_canceladas_999',
	'id_estado':'id_estado_canceladas_999'
}


#---------------------------------------------------PARAMETROS BASE NGN LARGA DISTANCIA NACIONAL -------------------------------------------------

#PARAMETROS NGN LARGA DISTANCIA NACIONAL
ruta_fuente_ngn_larga_distancia_nacional = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_ngn_larga_distancia_nacional ='Base de Ventas Segmento Pyme e Intermedias I y II CONSOLIDADO.xlsx'
nombre_hoja_ngn_larga_distancia_nacional ='NEGOCIOS'

#PARAMETROS MONITOREO
fuente_ngn_larga_distancia_nacional = f'{nombre_hoja_ngn_larga_distancia_nacional}'
destino_ngn_larga_distancia_nacional = 'tb_datos_crudos_ngn_larga_distancia_nacional'

mapeo_columnas_ngn_larga_distancia_nacional = {
    'id':'id_ngn_larga_distancia_nacional',
    'id_ejecucion': 'id_ejecucion_ngn_larga_distancia_nacional',
    'fecha': 'fecha_ngn_larga_distancia_nacional',
    'mes_digitacion': 'mes_digitacion_ngn_larga_distancia_nacional',
    'ano_digitacion': 'ano_digitacion_ngn_larga_distancia_nacional',
    'tipo_documento': 'tipo_documento_ngn_larga_distancia_nacional',
    'numero_documento': 'numero_documento_ngn_larga_distancia_nacional',
    'razon_social': 'razon_social_ngn_larga_distancia_nacional',
    'identificacion_consultor': 'identificacion_consultor_ngn_larga_distancia_nacional',
    'consultor': 'consultor_ngn_larga_distancia_nacional',
    'servicio': 'servicio_ngn_larga_distancia_nacional',
    'velocidad_plan': 'velocidad_plan_ngn_larga_distancia_nacional',
    'no_lineas': 'no_lineas_ngn_larga_distancia_nacional',
    'cargo_instalacion': 'cargo_instalacion_ngn_larga_distancia_nacional',
    'valor_servicio': 'valor_servicio_ngn_larga_distancia_nacional',
    'soporte_pc': 'soporte_pc_ngn_larga_distancia_nacional',
    'valor_recuperaciones': 'valor_recuperaciones_ngn_larga_distancia_nacional',
    'alquiler_equipos': 'alquiler_equipos_ngn_larga_distancia_nacional',
    'duracion_contrato': 'duracion_contrato_ngn_larga_distancia_nacional',
    'trm': 'trm_ngn_larga_distancia_nacional',
    'observaciones': 'observaciones_ngn_larga_distancia_nacional',
    'id_ngn': 'id_ngn_ngn_larga_distancia_nacional',
    'ot': 'ot',
    'no_contrato': 'no_contrato_ngn_larga_distancia_nacional',
    'fecha_firma_contrato': 'fecha_firma_contrato_ngn_larga_distancia_nacional',
    'tipo_venta': 'tipo_venta_ngn_larga_distancia_nacional',
    'ciudad_incidente': 'ciudad_incidente_ngn_larga_distancia_nacional',
    'departamento': 'departamento_ngn_larga_distancia_nacional',
    'red': 'red_ngn_larga_distancia_nacional',
    'ciudad_consultor': 'ciudad_consultor_ngn_larga_distancia_nacional',
    'regional': 'regional_ngn_larga_distancia_nacional',
    'canal': 'canal_ngn_larga_distancia_nacional',
    'proveedor': 'proveedor_ngn_larga_distancia_nacional',
    'cargo': 'cargo_ngn_larga_distancia_nacional',
    'usuario': 'usuario_ngn_larga_distancia_nacional',
    'segmento': 'segmento_ngn_larga_distancia_nacional',
    'division': 'division_ngn_larga_distancia_nacional',
    'manual_tarifas': 'manual_tarifas_ngn_larga_distancia_nacional',
    'campana_promocion': 'campana_promocion_ngn_larga_distancia_nacional',
    'ito': 'ito_ngn_larga_distancia_nacional',
    'servicio_ito': 'servicio_ito_ngn_larga_distancia_nacional',
    'familia': 'familia_ngn_larga_distancia_nacional',
    'clase': 'clase_ngn_larga_distancia_nacional',
    'componente': 'componente_ngn_larga_distancia_nacional',
    'coodinador_it': 'coodinador_it_ngn_larga_distancia_nacional',
    'consultor_it': 'consultor_it_ngn_larga_distancia_nacional',
    'notas_descripcion_fo': 'notas_descripcion_fo_ngn_larga_distancia_nacional',
    'id_oracle': 'id_oracle_ngn_larga_distancia_nacional',
    'fecha_procesamiento': 'fecha_procesamiento_ngn_larga_distancia_nacional',
    'fuente': 'fuente_ngn_larga_distancia_nacional',
    'id_estado': 'id_estado_ngn_larga_distancia_nacional'
}

#---------------------------------------------------PARAMETROS BASE RED RTTX -------------------------------------------------

#PARAMETROS RED RTTX
ruta_fuente_red_rttx = 'C:/01_Inteligencia_Comercial/Bases Genericas/'
nombre_archivo_red_rttx ='ESTRUCTURA MODELO GESTION INTEGRAL (RESIDENCIAL).xlsb'
nombre_hoja_red_rttx ='BASE'

#PARAMETROS MONITOREO
fuente_red_rttx = f'{nombre_archivo_red_rttx}'
destino_red_rttx = 'tb_datos_crudos_red_rttx'

mapeo_columnas_red_rttx = {
    'id':'id_red_rttx',
    'id_ejecucion':'id_ejecucion_red_rttx',
    'id_nodo':'id_nodo',
    'nombre_nodo': 'nombre_nodo_red_rttx',
    'comunidad': 'comunidad_red_rttx',
    'nombre_comunidad': 'nombre_comunidad_red_rttx',
    'departamento': 'departamento_red_rttx',
    'codigo_dane': 'codigo_dane_red_rttx',
    'estatus_rr': 'estatus_rr_red_rttx',
    'lanzamiento_comercial_nd': 'lanzamiento_comercial_nd_red_rttx',
    'red_nodo': 'red_nodo_red_rttx',
    'red_predominante': 'red_predominante_red_rttx',
    'hhpp': 'hhpp_red_rttx',
    'hogares': 'hogares_red_rttx',
    'servicios': 'servicios_red_rttx',
    'id_region': 'id_region_red_rttx',
    'region': 'region_red_rttx',
    'id_area': 'id_area_red_rttx',
    'area': 'area_red_rttx',
    'id_zona': 'id_zona_red_rttx',
    'zona': 'zona_red_rttx',
    'id_distrito': 'id_distrito_red_rttx',
    'distrito': 'distrito_red_rttx',
    'id_unidad_gestion': 'id_unidad_gestion_red_rttx',
    'unidad_gestion': 'unidad_gestion_red_rttx',
    'codigo': 'codigo_red_rttx',
    'aliado': 'aliado_red_rttx',
    'tipologia_red': 'tipologia_red_red_rttx',
    'estado_nodo': 'estado_nodo_red_rttx',
    'ancho_banda_retro': 'ancho_banda_retro_red_rttx',
    'id_opera': 'id_opera_red_rttx',
    'opera': 'opera_red_rttx',
    'largo': 'largo_red_rttx',
    'segmentacion': 'segmentacion_red_rttx',
    'fecha_procesamiento': 'fecha_procesamiento_red_rttx',
	'fuente': 'fuente_red_rttx',
	'id_estado':'id_estado_red_rttx'
}


#---------------------------------------------------PARAMETROS MONITOREO MACROS FO1 Y FO2-------------------------------------------------
destino_macrofo = 'tb_datos_crudos_fibra_optica'
macro= 'REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP'
#Columnas MACRO FO1 y FO2
mapeo_columnas_macrofo = {
    'id': 'id_macro',
    'id_ejecucion': 'id_ejecucion_macro',
    'cuenta': 'cuenta_legalizadas'
}
usuario_denodo='38501867'
contraseña_denodo='D1anap_24*'

#------------------------------------------------PARAMETROS PLANTA COMERCIAL  -------------------------------------------------

#PARAMETROS PLANTA COMERCIAL
#ruta_fuente_planta_comercial = 'C:/Users/INTCOM/OneDrive - Comunicacion Celular S.A.- Comcel S.A/Planta Comercial/'
ruta_fuente_planta_comercial = 'C:/Users/INTCOM/OneDrive - Comunicacion Celular S.A.- Comcel S.A/Planta Comercial/'
nombre_archivo_planta = 'PLANTA COMERCIAL NUEVA ESTRUCTURA.xlsx'
nombre_hoja_red_maestra = 'RED MAESTRA'
nombre_hoja_red_retiro = 'RETIRO'
nombre_hoja_red_retail = 'RETAIL'
nombre_hoja_red_directos = 'DIRECTOS'
nombre_hoja_red_cavs = 'CAV´S'
nombre_hoja_red_tmk = 'TMK'
nombre_hoja_red_inteligencia_comercial = 'INTELIGENCIA COMERCIAL'
nombre_hoja_red_inteligencia_comercial_retiro = 'INTELIGENCIA COMERCIAL RETIRO'
destino_planta_comercial = 'tb_datos_crudos_planta_comercial'
nombre_archivo_planta_ajuste = 'PLANTA_COMERCIAL.xlsx'

#---------------------------------------------------PARAMETROS BASE METAS-------------------------------------------------
#PARAMETROS METAS   
ruta_fuente_metas = 'C:/ambiente_desarrollo/dev-empresas-negocios-env/fuentes/base_metas/'
nombre_archivo_metas = 'Metas_Negocios.xlsx'
nombre_hoja_canal_fijo = 'Canales-FIJO'
nombre_hoja_canal_movil = 'Canales-MOVIL'
nombre_hoja_red = 'RED'
nombre_hoja_bajas_fijo = 'Bajas Fijo'

#PARAMETROS MONITOREO
fuente_metas = f'{nombre_hoja_canal_fijo} | {nombre_hoja_canal_movil} | {nombre_hoja_red} | {nombre_hoja_bajas_fijo}'
destino_metas = 'tb_datos_crudos_metas'

#-----------------------------------------------PARAMETROS BASE METAS OFICIAL---------------------------------------------
#PARAMETROS METAS   
ruta_fuente_metas_oficial= 'C:/ambiente_desarrollo/dev-empresas-negocios-env/fuentes/base_metas_oficial/'
nombre_archivo_metas_oficial = 'Metas_Negocios_Oficial.xlsx'
hoja_calculo_metas_oficial = 'Meta Negocios'

#PARAMETROS MONITOREO
fuente_metas = f'{hoja_calculo_metas_oficial}'
destino_metas_oficial = 'tb_datos_crudos_metas_oficial'

#-----------------------------------------------PARAMETROS METAS MOVIL EMPRESAS ---------------------------------------------
ruta_fuente_metas_empresas = 'C:/ambiente_desarrollo/dev-empresas-negocios-env/fuentes/base_metas_empresas/'
nombre_archivo_metas_empresas = 'Metas Directos Empresas.xlsx'
hoja_calculo_metas_empresas = 'FUERZA COMERCIAL MES'

#PARAMETROS MONITOREO
fuente_metas = f'{hoja_calculo_metas_empresas}'
destino_metas_empresas = 'tb_datos_crudos_metas_empresas'

##-----------------------------------------------PARAMETROS TRASNFERS TOTAL#----------------------------------------------
nombre_destino_transfers = 'tb_datos_crudos_transfers'
destino_transfers = 'fuentes_cruda.tb_datos_crudos_transfers'
mapeo_columnas_transfers=['id_ejecucion','cuenta','orden_trabajo','numero_contrato','nombres','tipo_documento','numero_documento','numero_telefono_1','numero_telefono_2','direccion_residencia','numero_apartamento',	\
                     'ciudad_venta','codigo_division','tipo_suscriptor','estado','tarifa','campana_1','campana_2','campana_3','codigo_servicio','nombre_servicio','descripcion_servicio','numero_dealer',	\
                     'nombre_dealer','grupo_dealer','coordinador','tipo_servicio','wo_kind','razon_servicio','nodo','nombre_nodo','wo_creador','fecha_creacion','fecha_completado','tipo_venta','val_dif_service',	\
                     'valor_servicio','renta_wo_anterior','renta_wo_actual','diferencia_renta','hora_creacion','numero_lineas_suscriptor','numero_servicios','origen_datos','estrato','numeral_2', \
                     'cod_black_list','desc_black_list','adicional_inf1','fecha_permanencia','segmento','especialista','area_gcia_vtas','zona_gcia_vtas','canal','aliado','poblacion','area_venta','zona_venta',	\
                     'distrito','tipo_red','grupo_servicio','linea','grupo_codigo','soho','base','llave','tipo','tipo_v','velocidad','servicio','fecha_cancelada','codigo_cancelacion','descripcion_cancelacion',	\
                     'estado_2','soporte_pc','servicios','proceso','descripcion_tarifa','valor_neto','id_estado','fuente']
             
nombre_fuentes_transfers='canceladas'+" | "+'instaladas'+" | "+'digitadas'+" | "+'999'+" | "+'UP'


#PARAMETROS INSTALADAS UP
ruta_fuente_instaladaup = 'fuentes_cruda.tb_datos_crudos_instaladas_up'
nombre_fuente_instaladasup ='tb_datos_crudos_instaladas_up'

#PARAMETROS MONITOREO
fuente_instaladasup = f'{nombre_fuente_instaladasup}'

nombre_destino_transfers = 'tb_datos_crudos_transfers'

#PARAMETROS DIGITADAS UP
ruta_fuente_digitadasup = 'fuentes_cruda.tb_datos_crudos_digitadas_up'
nombre_fuente_digitadasup ='tb_datos_crudos_digitadas_up'
nombre_fuente_digitadas = 'tb_datos_crudos_digitadas'

#Columnas instaladas
mapeo_columnas_instaladas_up_transfer = {
    'id_instaladas':'id',
    'id_ejecucion_instaladas':'id_ejecucion',
    'cuenta_instaladas':'cuenta',
    'ot':'orden_trabajo',
    'service_code':'codigo_servicio',
    'monthl_rental_charge_instaladas':'valor_servicio',
    'renta_mes_anterior_instaladas':'renta_wo_anterior',
    'cod_servicio_anterior_instaladas':'cod_servicio_anterior',
    'tipo_cambio_instaladas':'tipo_cambio',
    'fecha_procesamiento_instaladas': 'fecha_procesamiento',
	'fuente_instaladas': 'fuente',
	'id_estado_instaladas':'id_estado'
}

#Columnas digitadas
mapeo_columnas_digitadas_up_transfer = {
    'id':'id_digitadas',
    'id_ejecucion_digitadas':'id_ejecucion',
    'cuenta_digitadas':'cuenta',
    'ot':'orden_trabajo',
    'service_code':'codigo_servicio',
    'monthl_rental_charge_digitadas':'monthl_rental_charge',
    'renta_mes_anterior_digitadas':'renta_mes_anterior',
    'cod_servicio_anterior_digitadas':'cod_servicio_anterior',
    'tipo_cambio':'tipo_cambio_digitadas',
    'fecha_procesamiento_digitadas': 'fecha_procesamiento',
	'fuente_digitadas':'fuente',
	'id_estado_digitadas':'id_estado'
}




mapeo_transfers = {
    'id':'id_digitadas',
    'id_ejecucion':'id_ejecucion',
    'cuenta':'cuenta',
    'orden_trabajo':'orden_trabajo',
    'numero_contrato':'numero_contrato',
    'nombres':'nombres',
    'tipo_documento':'tipo_documento',
    'numero_documento':'numero_documento',
    'numero_telefono_1':'numero_telefono_1',
    'numero_telefono_2':'numero_telefono_2',
    'calle':'calle',
    'direccion_residencia':'direccion_residencia',
    'numero_apartamento':'numero_apartamento',
    'ciudad_venta':'ciudad_venta',
    'codigo_division':'codigo_division',
    'tipo_suscriptor':'tipo_suscriptor',
    'estado':'estado',
    'tarifa':'tarifa',
    'campana_1':'campana_1',
    'campana_2':'campana_2',
    'campana_3':'campana_3',
    'codigo_servicio':'codigo_servicio',
    'nombre_servicio':'nombre_servicio',
    'descripcion_servicio':'descripcion_servicio',
    'numero_dealer':'numero_dealer',
    'nombre_dealer':'nombre_dealer',
    'grupo_dealer':'grupo_dealer',
    'coordinador':'coordinador',
    'tipo_servicio':'tipo_servicio',
    'wo_kind':'wo_kind',
    'razon_servicio':'razon_servicio',
    'nodo':'nodo',
    'nombre_nodo':'nombre_nodo',
    'wo_creador':'wo_creador',
    'fecha_creacion':'fecha_creacion',
    'fecha_completado':'fecha_completado',
    'tipo_venta':'tipo_venta',
    'val_dif_service':'val_dif_service',
    'valor_servicio':'valor_servicio',
    'renta_wo_anterior':'renta_wo_anterior',
    'renta_wo_actual':'renta_wo_actual',
    'diferencia_renta':'diferencia_renta',
    'hora_creacion':'hora_creacion',
    'numero_lineas_suscriptor':'numero_lineas_suscriptor',
    'numero_servicios':'numero_servicios',
    'origen_datos':'origen_datos',
    'estrato':'estrato',
    'numeral_2':'numeral_2',
    'conyugue':'conyugue',
    'cod_black_list':'cod_black_list',
    'desc_black_list':'desc_black_list',
    'email':'email',
    'adicional_inf1':'adicional_inf1',
    'fecha_permanencia':'fecha_permanencia',
    'segmento':'segmento',
    'especialista':'especialista',
    'area_gcia_vtas':'area_gcia_vtas',
    'zona_gcia_vtas':'zona_gcia_vtas',
    'canal':'canal',
    'aliado':'aliado',
    'poblacion':'poblacion',
    'area_venta':'area_venta',
    'zona_venta':'zona_venta',
    'distrito':'distrito',
    'tipo_red':'tipo_red',
    'grupo_servicio':'grupo_servicio',
    'linea':'linea',
    'grupo_codigo':'grupo_codigo',
    'soho':'soho',
    'base':'base',
    'llave':'llave',
    'tipo':'tipo',
    'tipo_v':'tipo_v',
    'velocidad':'velocidad',
    'servicio':'servicio',
    'fecha_cancelada':'fecha_cancelada',
    'codigo_cancelacion':'codigo_cancelacion',
    'descripcion_cancelacion':'descripcion_cancelacion',
    'estado_2':'estado_2',
    'soporte_pc':'soporte_pc',
    'servicios':'servicios',
    'proceso':'proceso',
    'descripcion_tarifa':'descripcion_tarifa',
    'valor_neto':'valor_neto'  
}


#PARAMETROS INSTALADAS
ruta_fuente_instaladas_principal = 'fuentes_cruda.tb_datos_crudos_instaladas'
nombre_fuente_instaladas ='tb_datos_crudos_instaladas'

#PARAMETROS INSTALADAS 999
ruta_fuente_instaladas999 = 'fuentes_cruda.tb_datos_crudos_instaladas_999'
nombre_fuente_instaladas999 ='tb_datos_crudos_instaladas_999'

mapeo_columnas_instaladas_principal={
    'id':'id',
    'id_ejecucion':'id_ejecucion',
    'cuenta':'cuenta',
    'ot':'orden_trabajo',
    'numero_contrato':'numero_contrato',
    'nombres':'nombres',
    'tipo_documento':'tipo_documento',
    'numero_documento':'numero_documento',
    'numero_telefono_1':'numero_telefono_1',
    'numero_telefono_2':'numero_telefono_2',
    'numero_calle':'calle',
    'direccion_residencia':'direccion_residencia',
    'numero_apartamento':'numero_apartamento',
    'ciudad_venta':'ciudad_venta',
    'codigo_division':'codigo_division',
    'tipo_suscriptor':'tipo_suscriptor',
    'estado':'estado',
    'tarifa':'tarifa',
    'campana_1':'campana_1',
    'campana_2':'campana_2',
    'campana_3':'campana_3',
    'codigo_servicio':'codigo_servicio',
    'nombre_servicio':'nombre_servicio',
    'descripcion_servicio':'descripcion_servicio',
    'numero_dealer':'numero_dealer',
    'nombre_dealer':'nombre_dealer',
    'grupo_dealer':'grupo_dealer',
    'coordinador':'coordinador',
    'tipo_servicio':'tipo_servicio',
    'wo_kind':'wo_kind',
    'razon_servicio':'razon_servicio',
    'nodo':'nodo',
    'nombre_nodo':'nombre_nodo',
    'wo_creador':'wo_creador',
    'fecha_digitacion':'fecha_creacion',
    'fecha_completado':'fecha_completado',
    'tipo_venta':'tipo_venta',
    'val_dif_service':'val_dif_service',
    'valor_servicio':'valor_servicio',
    'renta_wo_anterior':'renta_wo_anterior',
    'renta_wo_actual':'renta_wo_actual',
    'diferencia_renta':'diferencia_renta',
    'numero_lineas_suscriptor':'numero_lineas_suscriptor',
    'numero_servicios':'numero_servicios',
    'origen_datos':'origen_datos',
    'estrato':'estrato',
    'numeral_2':'numeral_2',
    'conyugue':'conyugue',
    'cod_black_list':'cod_black_list',
    'desc_black_list':'desc_black_list',
    'email':'email',
    'adicional_inf1':'adicional_inf1',
    'fecha_permanencia':'fecha_permanencia',
    'especialista':'especialista',
    'area_gcia_vtas':'area_gcia_vtas',
    'zona_gcia_vtas':'zona_gcia_vtas',
    'canal':'canal',
    'aliado':'aliado',
    'poblacion':'poblacion',
    'area_venta':'area_venta',
    'zona_venta':'zona_venta',
    'distrito':'distrito',
    'tipo_red':'tipo_red',
    'grupo_servicio':'grupo_servicio',
    'fecha_procesamiento':'fecha_procesamiento',
    'fuente':'fuente',
    'id_estado':'id_estado'
}

mapeo_columnas_digitadas_principal={
    'id':'id',
    'id_ejecucion':'id_ejecucion',
    'cuenta':'cuenta',
    'ot':'orden_trabajo',
    'numero_contrato':'numero_contrato',
    'nombres':'nombres',
    'tipo_documento':'tipo_documento',
    'numero_documento':'numero_documento',
    'numero_telefono_1':'numero_telefono_1',
    'numero_telefono_2':'numero_telefono_2',
    'numero_calle':'calle',
    'direccion_residencia':'direccion_residencia',
    'numero_apartamento':'numero_apartamento',
    'ciudad_venta':'ciudad_venta',
    'codigo_division':'codigo_division',
    'tipo_suscriptor':'tipo_suscriptor',
    'estado':'estado',
    'tarifa':'tarifa',
    'campana_1':'campana_1',
    'campana_2':'campana_2',
    'campana_3':'campana_3',
    'codigo_servicio':'codigo_servicio',
    'nombre_servicio':'nombre_servicio',
    'descripcion_servicio':'descripcion_servicio',
    'numero_dealer':'numero_dealer',
    'nombre_dealer':'nombre_dealer',
    'grupo_dealer':'grupo_dealer',
    'coordinador':'coordinador',
    'tipo_servicio':'tipo_servicio',
    'wo_kind':'wo_kind',
    'razon_servicio':'razon_servicio',
    'nodo':'nodo',
    'nombre_nodo':'nombre_nodo',
    'wo_creador':'wo_creador',
    'fecha_digitacion':'fecha_creacion',
    'fecha_completado':'fecha_completado',
    'tipo_venta':'tipo_venta',
    'val_dif_service':'val_dif_service',
    'valor_servicio':'valor_servicio',
    'renta_wo_anterior':'renta_wo_anterior',
    'renta_wo_actual':'renta_wo_actual',
    'diferencia_renta':'diferencia_renta',
    'numero_lineas_suscriptor':'numero_lineas_suscriptor',
    'numero_servicios':'numero_servicios',
    'origen_datos':'origen_datos',
    'estrato':'estrato',
    'numeral_2':'numeral_2',
    'conyugue':'conyugue',
    'cod_black_list':'cod_black_list',
    'desc_black_list':'desc_black_list',
    'email':'email',
    'adicional_inf1':'adicional_inf1',
    'fecha_permanencia':'fecha_permanencia',
    'especialista':'especialista',
    'area_gcia_vtas':'area_gcia_vtas',
    'zona_gcia_vtas':'zona_gcia_vtas',
    'canal':'canal',
    'aliado':'aliado',
    'poblacion':'poblacion',
    'area_venta':'area_venta',
    'zona_venta':'zona_venta',
    'distrito':'distrito',
    'tipo_red':'tipo_red',
    'grupo_servicio':'grupo_servicio',
    'fecha_procesamiento':'fecha_procesamiento',
    'fuente':'fuente',
    'id_estado':'id_estado'
}


nombre_fuente_canceladas = 'tb_datos_crudos_canceladas'
mapeo_columnas_canceladas_principal={
    'id':'id',
    'id_ejecucion':'id_ejecucion',
    'cuenta':'cuenta',
    'ot':'orden_trabajo',
    'nombres':'nombres',
    'numero_telefono_1':'numero_telefono_1',
    'numero_telefono_2':'numero_telefono_2',
    'calle':'calle', 
    'direccion':'direccion_residencia',
    'numero_apartamento':'numero_apartamento',
    'ciudad_venta':'ciudad_venta',
    'codigo_division':'codigo_division',
    'tipo_suscriptor':'tipo_suscriptor',
    'estado':'estado',
    'codigo_servicio':'codigo_servicio',
    'nombre_servicio':'nombre_servicio',
    'descripcion_servicio':'descripcion_servicio',
    'valor_servicio':'valor_servicio',
    'numero_asesor':'numero_asesor',
    'nombre_asesor':'nombre_asesor',
    'tipo_orden':'tipo_orden',
    'usuario_creador':'usuario_creador',
    'fecha_creacion':'fecha_creacion',
    'hora_creacion':'hora_creacion',
    'fecha_cancelada':'fecha_cancelada',
    'hora_cancelada':'hora_cancelada',
    'migracion':'migracion',
    'codigo_cancelacion':'codigo_cancelacion',
    'descripcion_cancelacion':'descripcion_cancelacion',
    'estrato':'estrato',
    'numeral_2':'numeral_2',
    'conyugue':'conyugue',
    'cod_black_list':'cod_black_list',
    'desc_black_list':'desc_black_list',
    'email':'email',
    'inf_adici_01':'adicional_inf1',
    'fecha_permanencia':'fecha_permanencia',
    'distrito':'distrito',
    'tipo_red':'tipo_red',
    'grupo_servicio':'grupo_servicio',
    'fecha_procesamiento':'fecha_procesamiento',
    'fuente':'fuente',
    'id_estado':'id_estado'

}


nombre_fuente_canceladas999 = 'tb_datos_crudos_canceladas_999'
mapeo_columnas_canceladas999={
    'id':'id',
    'id_ejecucion':'id_ejecucion',
    'cuenta':'cuenta',
    'ot':'orden_trabajo',
    'nombres':'nombres',
    'numero_telefono_1':'numero_telefono_1',
    'numero_telefono_2':'numero_telefono_2',
    'calle':'calle', 
    'direccion':'direccion_residencia',
    'numero_apartamento':'numero_apartamento',
    'ciudad_venta':'ciudad_venta',
    'codigo_division':'codigo_division',
    'tipo_suscriptor':'tipo_suscriptor',
    'estado':'estado',
    'codigo_servicio':'codigo_servicio',
    'nombre_servicio':'nombre_servicio',
    'descripcion_servicio':'descripcion_servicio',
    'valor_servicio':'valor_servicio',
    'numero_asesor':'numero_asesor',
    'nombre_asesor':'nombre_asesor',
    'tipo_orden':'tipo_orden',
    'usuario_creador':'usuario_creador',
    'fecha_creacion':'fecha_creacion',
    'hora_creacion':'hora_creacion',
    'fecha_cancelada':'fecha_cancelada',
    'hora_cancelada':'hora_cancelada',
    'migracion':'migracion',
    'codigo_cancelacion':'codigo_cancelacion',
    'descripcion_cancelacion':'descripcion_cancelacion',
    'estrato':'estrato',
    'numeral_2':'numeral_2',
    'conyugue':'conyugue',
    'cod_black_list':'cod_black_list',
    'desc_black_list':'desc_black_list',
    'email':'email',
    'inf_adici_01':'adicional_inf1',
    'fecha_permanencia':'fecha_permanencia',
    'distrito':'distrito',
    'tipo_red':'tipo_red',
    'grupo_servicio':'grupo_servicio',
    'fecha_procesamiento':'fecha_procesamiento',
    'fuente':'fuente',
    'id_estado':'id_estado'

}