# Procesamiento de Denodo Digitadas RR

## Descripción 
Este módulo gestiona el procesamiento de datos de órdenes digitadas de RR desde Denodo.

### Scripts Principales

#### cargue_datos_crudos_digitadas_rr.py
- Procesa datos de órdenes digitadas RR
- Implementa transformaciones específicas
- Gestiona carga en base de datos
- Maneja validaciones y controles

### Funcionalidades Principales
- Extracción desde Denodo
- Validación de datos
- Transformaciones específicas
- Carga incremental
- Sistema de logging
- Control de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_denodo_digitadas_rr
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID ejecución
- Estados
- Contadores
- Errores
- Logs
