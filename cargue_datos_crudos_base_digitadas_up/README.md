# Procesamiento de Base Digitadas UP

## Descripción
Este módulo gestiona el procesamiento de datos de órdenes digitadas del sistema UP.

### Scripts Principales

#### cargue_datos_crudos_digitadas_up.py
- Procesa datos de órdenes digitadas UP
- Implementa transformaciones específicas 
- Gestiona carga en base de datos
- Maneja validaciones y controles

### Funcionalidades Principales
- Importación desde CSV
- Validación de datos
- Transformaciones específicas
- Carga incremental 
- Sistema de logging
- Control de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_digitadas_up
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID ejecución
- Estados
- Contadores
- Errores 
- Logs
