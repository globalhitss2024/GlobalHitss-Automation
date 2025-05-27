# Procesamiento de Fuentes DWH

## Descripción
Este módulo gestiona el procesamiento de datos desde el Data Warehouse.

### Scripts Principales

#### fuentes_dwh.py
- Procesa datos del DWH
- Implementa transformaciones específicas
- Gestiona carga en base de datos
- Maneja validaciones y controles

### Funcionalidades Principales
- Extracción desde DWH
- Transformación de datos
- Validación de registros
- Carga incremental
- Monitoreo de proceso
- Control de errores

### Tablas Relacionadas  
- fuentes_cruda.tb_datos_crudos_dwh
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID ejecución
- Estados
- Contadores 
- Errores
- Logs
