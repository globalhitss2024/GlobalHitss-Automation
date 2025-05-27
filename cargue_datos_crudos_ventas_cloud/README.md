# Procesamiento de Ventas Cloud

## Descripción
Este módulo gestiona el procesamiento de datos de ventas de servicios cloud.

### Scripts Principales

#### ventas_cloud.py
- Procesa datos de ventas cloud
- Implementa validaciones específicas  
- Gestiona carga en base de datos
- Maneja controles y monitoreo

### Funcionalidades Principales
- Importación desde archivos fuente
- Validaciones de negocio
- Transformación de datos
- Carga incremental
- Monitoreo de proceso
- Control de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_ventas_cloud
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID de proceso
- Estados
- Contadores
- Errores
- Logs
