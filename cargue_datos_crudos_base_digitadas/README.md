# Base Digitadas - Procesamiento de Datos

## Descripción 
Este módulo gestiona el procesamiento de datos de órdenes digitadas en el sistema.

### Scripts Principales

#### cargue_datos_crudos_digitadas.py
- Procesa datos crudos de órdenes digitadas
- Implementa proceso ETL completo
- Gestiona conexiones a PostgreSQL
- Incluye validaciones y transformaciones

### Funcionalidades Principales
- Carga de datos desde fuentes externas
- Limpieza y estandarización de datos
- Validación de duplicados
- Carga incremental a base de datos
- Sistema de logging y monitoreo
- Manejo de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_digitadas
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Controles Implementados
- Seguimiento de ejecución
- Validación de datos
- Control de duplicados
- Registro de errores
