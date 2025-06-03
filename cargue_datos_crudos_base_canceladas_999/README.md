# Procesamiento de Base Canceladas 999

## Descripción
Este módulo gestiona el procesamiento de datos de órdenes canceladas del sistema 999.

### Scripts Principales

#### cargue_datos_crudos_canceladas_999.py
- Procesa datos de órdenes canceladas 999
- Implementa transformaciones específicas
- Gestiona carga en base de datos
- Maneja validaciones y controles

### Funcionalidades Principales
- Importación desde fuentes
- Validación de datos
- Transformaciones específicas 
- Carga incremental
- Sistema de logging
- Control de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_canceladas_999
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID ejecución
- Estados 
- Contadores
- Errores
- Logs
