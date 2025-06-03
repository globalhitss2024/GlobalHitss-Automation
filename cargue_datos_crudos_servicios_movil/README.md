# Procesamiento de Servicios Móviles

## Descripción
Este módulo gestiona el procesamiento de datos relacionados con servicios móviles.

### Scripts Principales

#### fuentes_cruda_servicios_movil.py
- Procesa datos de servicios móviles
- Implementa transformaciones específicas
- Gestiona carga en base de datos PostgreSQL 
- Maneja validaciones y controles

### Funcionalidades Principales
- Importación de datos desde Excel
- Limpieza y homologación de servicios
- Validación de registros duplicados
- Carga incremental a base de datos
- Sistema de logging
- Manejo de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_servicios_movil
- control_procesamiento.tb_resumen_cargue  
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID de ejecución único
- Estados de procesamiento
- Contadores de registros
- Tracking de errores
- Logging de procesos
