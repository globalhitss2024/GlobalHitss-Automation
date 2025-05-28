# Procesamiento de Base Maestro Códigos 

## Descripción
Este módulo gestiona el procesamiento de datos del maestro de códigos de servicios.

### Scripts Principales

#### cargue_datos_crudos_maestro_codigos.py
- Procesa datos del maestro de códigos
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
- Manejo de códigos duplicados

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_maestro_codigos
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID ejecución
- Estados de procesamiento 
- Contadores de registros
- Control de errores
- Logs del proceso
