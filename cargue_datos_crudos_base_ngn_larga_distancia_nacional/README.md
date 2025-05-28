# Procesamiento de Base NGN Larga Distancia Nacional

## Descripción
Este módulo gestiona el procesamiento de datos de servicios de larga distancia nacional NGN.

### Scripts Principales

#### cargue_datos_crudos_base_ngn_larga_distancia_nacional.py
- Procesa datos de larga distancia nacional
- Implementa transformaciones específicas
- Gestiona carga en base de datos
- Maneja validaciones y controles

### Funcionalidades Principales
- Importación desde Excel
- Validación de datos
- Transformaciones específicas
- Carga incremental
- Sistema de logging
- Control de errores
- Manejo de fechas
- Validación de montos

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_ngn_larga_distancia_nacional
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID ejecución
- Estados de procesamiento
- Contadores de registros
- Control de errores
- Logs del proceso
