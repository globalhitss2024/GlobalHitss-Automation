# Procesamiento de Ventas Manuales

## Descripción
Este módulo gestiona el procesamiento de datos de ventas registradas manualmente.

### Scripts Principales

#### ventas_manuales.py
- Procesa datos de ventas manuales
- Realiza validaciones de negocio
- Gestiona carga en base de datos
- Implementa controles de calidad

### Funcionalidades Principales
- Importación desde Excel
- Validación de información
- Transformación de datos
- Carga incremental 
- Monitoreo del proceso
- Control de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_ventas_manuales
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control  
- ID de ejecución
- Estados de proceso
- Contadores
- Tracking de errores
- Logs
