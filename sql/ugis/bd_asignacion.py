class UgiQueries:

    def query_asignacion():
        return """
        select nit, nit_a,cfm_movil,segmento_actual,jefe, gerente, ciudad, departamento,recurrente_fo_hfc,razon_social,fecha_asignacion,estado_cliente_fym
        from(
        select "A.NIT" as nit,
            "A.NIT" as nit_a,
            "A.CFM_MOVIL" as cfm_movil,
            "A.SEGMENTO_ACTUAL" as segmento_actual,
            "A.JEFE_ACTUAL_FIJO" as jefe,
            "A.GERENTE_ACTUAL_FIJO" as gerente, 
            "A.CIUDAD" as ciudad, 
            "A.DEPARTAMENTO" as departamento,
            "A.TOTAL_RECURRENTE_FO_Y_HFC" recurrente_fo_hfc,
            "A.RAZON_SOCIAL" as razon_social,
            "A.FECHA_ASIGNACION_ACTUAL_FIJO" as fecha_asignacion, 
            "A.ESTADO_CLIENTE_FYM" as estado_cliente_fym,
            row_number() over(partition by "A.NIT"
                                    order  by "A.INSERT_DATE" desc) as rn
        from bd_production.tb_consulta_general_final
        )a
        where rn = 1
        order by nit desc
        """
    def query_seg_homologo():
        return """
        select nombre_asignacion,
               segmento_homologado as segmento,
               direccion_homologada  as direccion
        from public.tbl_homologo_seg_asignacion
        """