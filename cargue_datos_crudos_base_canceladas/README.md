# Base Canceladas - Procesamiento de Datos

## Descripción
Este módulo procesa datos de órdenes canceladas para el sistema de inteligencia comercial.

### Scripts Principales

#### cargue_datos_crudos_canceladas.py
- Procesa datos crudos de órdenes canceladas
- Realiza ETL (Extracción, Transformación y Carga)
- Gestiona conexiones a PostgreSQL
- Incluye manejo de errores y logging

### Funcionalidades Principales
- Importación de datos desde Excel
- Limpieza y transformación de datos
- Validación de registros duplicados
- Carga en base de datos PostgreSQL
- Monitoreo y logging de ejecución
- Control de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_canceladas
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID de ejecución único
- Fechas de procesamiento
- Estados de ejecución
- Contadores de registros
- Tracking de errores
